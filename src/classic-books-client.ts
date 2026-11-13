import type { BookSuggestion } from './book-suggestion.js';

/** Thrown when ClassicBooks does not return a book. */
export class ClassicBooksError extends Error {
  readonly status: number;

  constructor(status: number) {
    super(`ClassicBooks responded with HTTP ${status}`);
    this.name = 'ClassicBooksError';
    this.status = status;
  }
}

/** GET /ClassicBooks/random: asks ClassicBooks for a book, as JSON. */
export async function fetchRandomBook(baseUrl: string): Promise<BookSuggestion> {
  const response = await fetch(`${baseUrl.replace(/\/+$/, '')}/ClassicBooks/random`, {
    headers: { Accept: 'application/json' },
  });
  if (!response.ok) {
    throw new ClassicBooksError(response.status);
  }
  return (await response.json()) as BookSuggestion;
}
