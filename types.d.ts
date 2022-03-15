export interface QueryOptions {
  lastKey?: string;
  app?: string;
  user?: string;
  limit?: number;
}

export interface IndexQueryOptions extends QueryOptions {
  lastAppKey?: string;
  lastUserKey?: string;
}

export interface IndexQueryResult {
  lastAppKey?: string;
  lastUserKey?: string;
  lastKey?: string;
  data: any[];
}

export interface IndexKeysResult {
  keys: string[];
  lastKey?: string;
}
