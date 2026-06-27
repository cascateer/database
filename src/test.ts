import { createTable } from "./table";

interface TestTableRecord {
  id: string;
  name?: string;
}

export const TestTable = createTable<TestTableRecord, "id">(
  "test",
  "id",
  (ids) => ids.map((id) => ({ id })),
);

new TestTable().accessSome(["foo"]).then(console.log);
new TestTable().accessSome(["foo"]).then(console.log);
