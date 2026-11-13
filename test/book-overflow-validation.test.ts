import { randomUUID } from 'node:crypto';
import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import { after, before, describe, it } from 'node:test';
import { createBookOverflowServer } from '../src/book-overflow.js';
import { SuggestionPublisher } from '../src/suggestion-publisher.js';
import { createTopic, expectNoValue } from './support/kafka.js';
import { brokersFromEnvironment, schemaRegistryUrlFromEnvironment } from './support/stack.js';
import { CLASSIC_BOOKS_URL, ClassicBooksStub } from './support/classic-books-stub.js';

const SCHEMA_FILE = new URL('../schemas/dont_forget_message.avsc', import.meta.url);
const BOOK = {
  name: 'Refactoring',
  edition: '1st',
  format: 'paperback',
  authors: ['Fowler Martin', 'Beck Kent'],
};

describe('BookOverflowResourceTest', () => {
  let brokers: string[];
  let publisher: SuggestionPublisher;
  let server: Server;
  let appUrl: string;
  let topic: string;
  let classicBooks: ClassicBooksStub;

  before(async () => {
    brokers = brokersFromEnvironment();
    const schemaRegistryUrl = schemaRegistryUrlFromEnvironment();

    // A topic of its own, so the test sees only what this test's requests write to it.
    topic = `SuppliedSuggestionsTopic-${randomUUID().slice(0, 8)}`;
    await createTopic(brokers, topic);

    publisher = new SuggestionPublisher({ brokers, schemaRegistryUrl, topic, schemaFile: SCHEMA_FILE });
    await publisher.connect();

    server = createBookOverflowServer({ classicBooksUrl: CLASSIC_BOOKS_URL, publisher });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    appUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

    classicBooks = new ClassicBooksStub();
    classicBooks.start([schemaRegistryUrl]);
    classicBooks.passThrough(appUrl);
  }, { timeout: 180_000 });

  after(async () => {
    classicBooks?.stop();
    server?.close();
    await publisher?.disconnect();
  });

  it('rejectsARequestWithoutAnEmail', async () => {
    givenClassicBooksHasABook();

    const response = await whenAReaderAsksWithoutAnEmail();

    thenTheRequestIsRejected(response);
    thenClassicBooksIsNotCalled();
    await thenNothingIsPublished();
  });

  // Given: ClassicBooks has a book, so a call to it would succeed.
  function givenClassicBooksHasABook(): void {
    classicBooks.answersWith(BOOK);
  }

  // When: a reader asks BookOverflow over HTTP, with no email on the request.
  async function whenAReaderAsksWithoutAnEmail(): Promise<{ status: number; body: string }> {
    const response = await fetch(`${appUrl}/BookOverflow/suggestion`);
    return { status: response.status, body: await response.text() };
  }

  // Then: BookOverflow answers 400, and says the email is required.
  function thenTheRequestIsRejected(response: { status: number; body: string }): void {
    assert.equal(response.status, 400);
    assert.deepEqual(JSON.parse(response.body), { message: 'The email parameter is required' });
  }

  // Then: BookOverflow does not ask ClassicBooks for a book.
  function thenClassicBooksIsNotCalled(): void {
    assert.equal(classicBooks.callCount(), 0);
  }

  // Then: nothing is written to the topic.
  async function thenNothingIsPublished(): Promise<void> {
    await expectNoValue(brokers, topic);
  }
});
