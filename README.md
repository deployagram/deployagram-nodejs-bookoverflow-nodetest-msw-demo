# Node.js BookOverflow demo (node:test, MSW)

The Node.js version of the Java BookOverflow app in `deployagram-quarkus-bookoverflow-avro-demo`. It listens for
HTTP requests for a book recommendation, asks ClassicBooks over HTTP for a book, publishes the suggestion to Kafka as
an Avro message, and responds to the original request with the book.

## The application

- `GET /BookOverflow/suggestion?email=…` (the HTTP server) returns a book, as JSON. It answers 400 if there is no
  email, and 404 for any other path.
- It calls `GET /ClassicBooks/random` (the HTTP client), with `Accept: application/json`.
- It publishes `{ email, suggestion }` to `SuppliedSuggestionsTopic` as an Avro message. The schema is
  `schemas/dont_forget_message.avsc`, the Java demo's schema, registered with the Confluent Schema Registry.

## Tests

```bash
npm install
npm test
```

Docker must be running. `npm test` starts Kafka and the Schema Registry once, through `test/with-stack.ts`, runs
`node:test` with their addresses in the environment, and stops the containers at the end. Each test file runs in its
own process, which is why the containers are started outside the runner.

- `test/book-overflow.test.ts` calls the application over HTTP, stubs ClassicBooks with MSW, and checks the response
  and the Avro message on the topic.
- `test/book-overflow-validation.test.ts` sends a request with no email. It checks for a 400, that ClassicBooks is
  not called, and that nothing is written to the topic.

ClassicBooks is stubbed in-process by `test/support/classic-books-stub.ts`. MSW 3 works with `node:test` because the
tests run through `tsx`, which loads ES modules.

## Decisions

- **Containers start once, outside the runner.** Node 22's test runner has no global setup, and it runs each test file
  in its own process. `test/with-stack.ts` owns the containers instead.

- **Hook timeouts are options.** `node:test` takes a hook's timeout as its second argument, not from `this.timeout`.

- **MSW's option is `onUnhandledFrame`.** MSW 3 renamed `onUnhandledRequest` to `onUnhandledFrame`.

- **The plain topic name.** BookOverflow publishes to `SuppliedSuggestionsTopic`, as the Java BookOverflow demo does.

- **Kafka image.** `apache/kafka-native` does not work with testcontainers' Kafka module, which starts the broker with
  `/etc/confluent/docker/run`. That script is only in Confluent's images, so the tests use `confluentinc/cp-kafka:7.6.1`
  in KRaft mode.
