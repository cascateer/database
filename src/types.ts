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

interface TableActions<R, K extends keyof R> {
  one: {
    payload: never;
    dispatch: [id: R[K], predicate: Function1<R, void>];
  };
  all: {
    payload: never;
    dispatch: [predicate: Function1<R[], void>];
  };
  insert: {
    payload: {
      records: R[];
    };
    dispatch: [predicate: Function1<R[K][], MaybePromise<R[]>>];
  };
  update: {
    payload: {
      record: R;
    };
    dispatch: [id: R[K], predicate: Function1<R, MaybePromise<R>>];
  };
  delete: {
    payload: {
      id: R[K];
    };
    dispatch: [id: R[K]];
  };
}

export type TableAction<
  R,
  K extends keyof R = keyof R,
  Type extends keyof TableActions<R, K> = keyof TableActions<R, K>,
> = BaseTableAction<Type> &
  {
    [Type in keyof TableActions<R, K>]: {
      type: Type;
      payload: TableActions<R, K>[Type]["payload"];
      args?: [Type, ...TableActions<R, K>[Type]["dispatch"]];
    };
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
