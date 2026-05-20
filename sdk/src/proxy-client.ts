import type { ProxyResponse } from './types';

export class ProxyClient {
  private url: string;
  private fetchFn: typeof globalThis.fetch;

  constructor(url: string, fetchFn: typeof globalThis.fetch = globalThis.fetch) {
    this.url = url;
    this.fetchFn = fetchFn;
  }

  async post(sql: string, dialect: string, datasource: string = 'default'): Promise<ProxyResponse> {
    const response = await this.fetchFn(this.url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql, dialect, datasource }),
    });

    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      throw new Error(
        (body as { error?: string }).error ?? `Proxy returned ${response.status}`,
      );
    }

    return response.json() as Promise<ProxyResponse>;
  }
}
