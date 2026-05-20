import type { StorageEngine } from './types';

export class IDBStorage implements StorageEngine {
  private store: typeof import('idb-keyval') | null = null;

  private async getStore() {
    if (!this.store) {
      this.store = await import('idb-keyval');
    }
    return this.store;
  }

  async get(key: string): Promise<Uint8Array | undefined> {
    const store = await this.getStore();
    return store.get(key);
  }

  async set(key: string, value: Uint8Array): Promise<void> {
    const store = await this.getStore();
    await store.set(key, value);
  }
}
