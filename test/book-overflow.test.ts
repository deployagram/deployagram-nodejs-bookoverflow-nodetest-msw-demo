import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import { after, afterEach, before, describe, it } from 'node:test';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { recordProducedMessages } from '@deployagram/kafkajs';
import { DiagramLoggerClient } from '@deployagram/logger-client';
import type { HttpRecorder } from '@deployagram/msw';
import { createBookOverflowServer } from '../src/book-overflow.js';
import { SuggestionPublisher } from '../src/suggestion-publisher.js';
import { readFirstValue, SUGGESTIONS_TOPIC } from './support/kafka.js';
import { brokersFromEnvironment, schemaRegistryUrlFromEnvironment } from './support/stack.js';
import { callRecordingInbound } from './support/inbound-call.js';
import { CLASSIC_BOOKS_URL, ClassicBooksStub } from './support/classic-books-stub.js';
import { useDeployagram } from './support/deployagram.js';

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
  let logger: DiagramLoggerClient;
  let publisher: SuggestionPublisher;
  let server: Server;
  let appUrl: string;
  let classicBooks: ClassicBooksStub;
  let recorder: HttpRecorder | undefined;

  // The recorder is flushed before the test's end is marked. This hook is registered before the Deployagram adapter's,
  // and node:test runs afterEach hooks in registration order.
  afterEach(async () => {
    await recorder?.flush();
  });

  // Uses the Collector that the run started. There is no Proxy: ClassicBooks is stubbed in-process with MSW, and the
  // test records its own inbound call.
  const deployagram = useDeployagram({
    name: 'BookOverflowResourceTest',
    environment: {
      proxy: false,
      loggerEnvironment: { KAFKA_BROKER_LOCATION_MODE: 'RUNTIME', SCHEMA_REGISTRY_LOCATION_MODE: 'RUNTIME' },
    },
  });

  before(async () => {
    brokers = brokersFromEnvironment();
    const schemaRegistryUrl = schemaRegistryUrlFromEnvironment();
    registry = new SchemaRegistry({ host: schemaRegistryUrl });

    logger = new DiagramLoggerClient({ baseUrl: deployagram.collectorUrl });

    // Each acknowledged record is reported to the Collector, as the Java producer interceptor reports it.
    publisher = new SuggestionPublisher({
      brokers,
      schemaRegistryUrl,
      topic: SUGGESTIONS_TOPIC,
      schemaFile: SCHEMA_FILE,
      wrapProducer: (producer) => recordProducedMessages(producer, { logger, producingApp: 'BookOverflow' }),
    });
    await publisher.connect();

    server = createBookOverflowServer({ classicBooksUrl: CLASSIC_BOOKS_URL, publisher });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    appUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

    // The Collector, the Schema Registry and the application are real servers, so their calls pass through MSW.
    classicBooks = new ClassicBooksStub();
    classicBooks.start([deployagram.collectorUrl, schemaRegistryUrl]);
    classicBooks.passThrough(appUrl);
  }, { timeout: 180_000 });

  after(async () => {
    recorder?.stop();
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

  // Given: ClassicBooks has a book to give, and the call to it is recorded to the Collector.
  function givenClassicBooksHasABook(): void {
    classicBooks.answersWith(BOOK);
    recorder = classicBooks.record(logger, 'BookOverflow');
  }

  // When: AppA calls BookOverflow over HTTP for a suggestion for the reader's email.
  async function whenAReaderAsksForABook(): Promise<{ status: number; body: string }> {
    const url = new URL(`${appUrl}/BookOverflow/suggestion`);
    url.searchParams.set('email', EMAIL);
    return callRecordingInbound(logger, { caller: 'AppA', callee: 'BookOverflow' }, url);
  }

  // Then: AppA gets the book back, as JSON.
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
