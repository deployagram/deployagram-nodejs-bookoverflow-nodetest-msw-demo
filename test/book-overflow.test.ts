import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import { after, before, describe, it } from 'node:test';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { createBookOverflowServer } from '../src/book-overflow.js';
import { SuggestionPublisher } from '../src/suggestion-publisher.js';
import { readFirstValue, SUGGESTIONS_TOPIC } from './support/kafka.js';
import { brokersFromEnvironment, schemaRegistryUrlFromEnvironment } from './support/stack.js';
import { CLASSIC_BOOKS_URL, ClassicBooksStub } from './support/classic-books-stub.js';

const SCHEMA_FILE = new URL('../schemas/dont_forget_message.avsc', import.meta.url);
const EMAIL = 'test@deployagram.com';
const BOOK = {
  name: 'Refactoring',
  edition: '1st',
  format: 'paperback',
  authors: ['Fowler Martin', 'Beck Kent'],
};

describe('BookOverflowResourceTest', () => {
  let brokers: string[];
  let registry: SchemaRegistry;
  let publisher: SuggestionPublisher;
  let server: Server;
  let appUrl: string;
  let classicBooks: ClassicBooksStub;

  before(async () => {
    brokers = brokersFromEnvironment();
    const schemaRegistryUrl = schemaRegistryUrlFromEnvironment();
    registry = new SchemaRegistry({ host: schemaRegistryUrl });

    publisher = new SuggestionPublisher({
      brokers,
      schemaRegistryUrl,
      topic: SUGGESTIONS_TOPIC,
      schemaFile: SCHEMA_FILE,
    });
    await publisher.connect();

    server = createBookOverflowServer({ classicBooksUrl: CLASSIC_BOOKS_URL, publisher });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    appUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

    // The application and the Schema Registry are real servers, so their calls pass through MSW to them.
    classicBooks = new ClassicBooksStub();
    classicBooks.start([schemaRegistryUrl]);
    classicBooks.passThrough(appUrl);
  }, { timeout: 180_000 });

  after(async () => {
    classicBooks?.stop();
    server?.close();
    await publisher?.disconnect();
  });

  it('callCForBook', async () => {
    givenClassicBooksHasABook();

    const response = await whenAReaderAsksForABook();

    thenTheReaderGetsTheBook(response);
    await thenTheSuggestionIsPublished();
  });

  // Given: ClassicBooks has a book to give, and the application's call to it must ask for JSON.
  function givenClassicBooksHasABook(): void {
    classicBooks.answersWith(BOOK);
  }

  // When: a reader asks BookOverflow over HTTP for a suggestion for their email.
  async function whenAReaderAsksForABook(): Promise<{ status: number; body: string }> {
    const url = new URL(`${appUrl}/BookOverflow/suggestion`);
    url.searchParams.set('email', EMAIL);
    const response = await fetch(url);
    return { status: response.status, body: await response.text() };
  }

  // Then: the reader gets the book back, as JSON.
  function thenTheReaderGetsTheBook(response: { status: number; body: string }): void {
    assert.equal(response.status, 200);
    assert.deepEqual(JSON.parse(response.body), BOOK);
  }

  // Then: the suggestion for the reader is on the topic, as an Avro message.
  async function thenTheSuggestionIsPublished(): Promise<void> {
    // The registry returns Avro records, which are class instances. Compare their plain JSON.
    const decoded = await registry.decode(await readFirstValue(brokers, SUGGESTIONS_TOPIC));
    assert.deepEqual(JSON.parse(JSON.stringify(decoded)), { email: EMAIL, suggestion: BOOK });
  }
});
