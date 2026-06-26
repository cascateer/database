import { Table } from "./table";

const testTable = new Table("test");

testTable.accessSome(["foo"]).then(console.log);
