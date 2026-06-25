export interface TableDefinition<T> {
  key: string;
  type: T;
}

export interface Tables extends Record<keyof {}, TableDefinition<any>> {
  test: TableDefinition<{
    id: string;
    name?: string;
  }>;
}
