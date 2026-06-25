import { Table } from "./table";

const testTable = new Table("test", "id", (ids) => ids.map((id) => ({ id })));

testTable.accessSome(["foo"]).then(console.log);
