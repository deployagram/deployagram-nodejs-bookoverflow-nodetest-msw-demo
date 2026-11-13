import { http, HttpResponse, passthrough } from 'msw';
import { setupServer } from 'msw/node';
import type { DiagramLogger } from '@deployagram/api';
import { recordHttp, type HttpRecorder } from '@deployagram/msw';
import type { BookSuggestion } from '../../src/book-suggestion.js';

/** ClassicBooks, as BookOverflow calls it. MSW stubs this address in-process, so nothing listens here. */
export const CLASSIC_BOOKS_URL = 'http://localhost:9050';
export const CLASSIC_BOOKS_PATH = '/ClassicBooks/random';

/**
 * ClassicBooks, stubbed in-process with MSW. It answers GET /ClassicBooks/random with the book the test gives it, as
 * JSON, and only to a request that asks for JSON. It counts the calls it gets. Calls to the given URLs pass through to
 * their real servers.
 */
export class ClassicBooksStub {
  private book: BookSuggestion | undefined;
  private calls = 0;
  private readonly server = setupServer(
    http.get(`${CLASSIC_BOOKS_URL}${CLASSIC_BOOKS_PATH}`, ({ request }) => {
      this.calls += 1;
      if (request.headers.get('accept') !== 'application/json' || this.book === undefined) {
        return new HttpResponse(null, { status: 406 });
      }
      return HttpResponse.json(this.book);
    }),
  );

  /** Given: ClassicBooks has this book to give. */
  answersWith(book: BookSuggestion): void {
    this.book = book;
  }

  /** How many calls ClassicBooks has had. */
  callCount(): number {
    return this.calls;
  }

  /** Starts stubbing. Calls to the given URLs pass through to their real servers. */
  start(passThroughUrls: string[]): void {
    this.server.use(...passThroughUrls.map((url) => http.all(`${url}/*`, () => passthrough())));
    this.server.listen({ onUnhandledFrame: 'error' });
  }

  /** Records each call to ClassicBooks to the Collector. Returns the recorder, which the test flushes after each test. */
  record(logger: DiagramLogger, caller: string): HttpRecorder {
    return recordHttp(this.server, {
      logger,
      caller,
      callees: [{ name: 'ClassicBooks', urlPrefix: CLASSIC_BOOKS_URL }],
    });
  }

  /** Lets calls to the given URL, such as the application's own HTTP server, reach it. */
  passThrough(url: string): void {
    this.server.use(http.all(`${url}/*`, () => passthrough()));
  }

  stop(): void {
    this.server.close();
  }
}
