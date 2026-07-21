import { MaybePromise } from "@cascateer/lib";
import { LazyPromise } from "@cascateer/lib/promise";
import { Function1 } from "lodash";
import { Ora } from "ora";
import { File } from "./file";
import { Table } from "./table";

interface BaseTableAction<Type> {
  id: string;
  previousId?: string;
  type: Type;
  payload: unknown;
  args?: [Type, ...unknown[]];
}

export interface TableActionPayloads<R, K extends keyof R> {
  one: never;
  all: never;
  insert: {
    records: R[];
  };
  update: {
    record: R;
  };
  delete: {
    id: R[K];
  };
}

export type TableAction<
  R,
  K extends keyof R = keyof R,
  Type extends keyof TableActionPayloads<R, K> = keyof TableActionPayloads<
    R,
    K
  >,
> = BaseTableAction<Type> &
  {
    [T in Type]: {
      type: T;
      payload: TableActionPayloads<R, K>[T];
    };
  }[Type];

export interface TableActionDispatchArgs<R, K extends keyof R> {
  one: [id: R[K], predicate: Function1<R | undefined, void>];
  all: [predicate: Function1<R[], void>];
  insert: [predicate: Function1<R[K][], MaybePromise<R[]>>];
  update: [id: R[K], predicate: Function1<R, MaybePromise<R>>];
  delete: [id: R[K]];
}

export type TableActionDispatchArgsUnion<
  R,
  K extends keyof R,
  Type extends keyof TableActionDispatchArgs<R, K> =
    keyof TableActionDispatchArgs<R, K>,
> = {
  [T in Type]: [T, ...TableActionDispatchArgs<R, K>[T]];
}[Type];

export interface TableActionCreator<R, K extends keyof R> extends LazyPromise<
  R[],
  TableActionCreatorResult<R, K>
> {}

export interface TableActionCreatorResult<R, K extends keyof R> {
  actions: TableAction<R, K>[];
  callback?: Function1<R[], void>;
}

export interface TableRecordCreator<R, K extends keyof R> {
  (ids: R[K][], spinner?: Ora): MaybePromise<R[]>;
}

export interface FileTableRecord {
  url: string;
  name: string;
  checksum: string;
}

export interface FileTable extends Table<FileTableRecord, "url"> {
  getFile: (url: string, spinner?: Ora) => Promise<File>;
}
