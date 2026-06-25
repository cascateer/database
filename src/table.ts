import { findDupeBy, MaybePromise, nonNullable } from "@cascateer/lib";
import { LazyPromise } from "@cascateer/lib/promises";
import assert from "assert";
import { readdir, readFile, writeFile } from "fs/promises";
import {
  difference,
  Function1,
  intersectionBy,
  thru,
  uniq,
  without,
} from "lodash";
import { Ora } from "ora";
import { resolve } from "path";
import { mergeMap, Subject } from "rxjs";
import { v4 } from "uuid";
import {
  reduceActions,
  TableActionCreator,
  TableActionCreatorResult,
} from "./observables/reduceActions";
import { Tables } from "./types";

interface BaseTableAction<Type> {
  id: string;
  previousId?: string;
  type: Type;
  payload: unknown;
  args?: [Type, ...unknown[]];
}

interface TableActions<T, K extends keyof T> {
  one: {
    payload: never;
    dispatch: [id: T[K], predicate: Function1<T, void>];
  };
  all: {
    payload: never;
    dispatch: [predicate: Function1<T[], void>];
  };
  insert: {
    payload: {
      records: T[];
    };
    dispatch: [predicate: Function1<T[K][], MaybePromise<T[]>>];
  };
  update: {
    payload: {
      record: T;
    };
    dispatch: [id: T[K], predicate: Function1<T, MaybePromise<T>>];
  };
  delete: {
    payload: {
      id: T[K];
    };
    dispatch: [id: T[K]];
  };
}

export type TableAction<
  T,
  K extends keyof T = keyof T,
  Type extends keyof TableActions<T, K> = keyof TableActions<T, K>,
> = BaseTableAction<Type> &
  {
    [Type in keyof TableActions<T, K>]: {
      type: Type;
      payload: TableActions<T, K>[Type]["payload"];
      args?: [Type, ...TableActions<T, K>[Type]["dispatch"]];
    };
  }[Type];

export class Table<
  Id extends keyof Tables,
  T extends Tables[Id]["type"] = Tables[Id]["type"],
  K extends Tables[Id]["key"] & keyof T = Tables[Id]["key"] & keyof T,
> {
  private static readonly BASE_URL = resolve(__dirname, "../tables");

  private static readonly locks: Partial<
    Record<keyof Tables, Subject<TableActionCreator<any, any>>>
  > = {};

  private readonly locks: Subject<TableActionCreator<T, K>>;

  applyActions(records: T[], ...actions: TableAction<T, K>[]) {
    return actions.reduce((records, action) => {
      switch (action.type) {
        case "insert":
          return records.concat(action.payload.records);
        case "update": {
          const targetRecord = action.payload.record;

          return records.map((record) =>
            this.selectId(record) === this.selectId(targetRecord)
              ? targetRecord
              : record,
          );
        }
        case "delete":
          return without(records, this.selectById(records)(action.payload.id));
      }

      return records;
    }, records);
  }

  constructor(
    public id: Id,
    public key: K,
    public createSome: (ids: T[K][], spinner?: Ora) => MaybePromise<T[]>,
  ) {
    this.locks = Table.locks[this.id] ??= new Subject<
      TableActionCreator<T, K>
    >();

    this.locks
      .pipe(
        reduceActions(this.applyActions, this.readActions),
        mergeMap((action) =>
          writeFile(resolve(this.path, `${v4()}.json`), JSON.stringify(action)),
        ),
      )
      .subscribe();
  }

  get path() {
    return resolve(Table.BASE_URL, this.id);
  }

  private readonly readActions = new LazyPromise<
    T[],
    TableActionCreatorResult<T, K>
  >(
    () =>
      new Promise((callback) =>
        readdir(this.path).then(async (files) => {
          const actions = new Array<TableAction<T, K>>();

          for (const file of files) {
            actions.push(
              await readFile(resolve(this.path, file), "utf-8").then<
                TableAction<T, K>
              >(JSON.parse),
            );
          }

          return {
            actions: actions.reduce(
              (chainedActions, action, _, actions) =>
                chainedActions.concat(
                  actions.find(({ previousId }) => previousId === action.id) ??
                    [],
                ),
              [
                actions.find((action) => action.previousId == null) ?? [],
              ].flat(),
            ),
            callback,
          };
        }),
      ),
  );

  private readonly writeRecords = new LazyPromise<T[]>((records) =>
    writeFile(this.path, JSON.stringify(records, null, "\t")).then(
      () => records,
    ),
  );

  selectId = (record: T): T[K] => record[this.key];
  selectById =
    (records: T[]) =>
    (id: T[K]): T => (
      assert(findDupeBy(records, this.selectId) == null),
      nonNullable(records.find((record) => this.selectId(record) === id))
    );

  public async dispatch(
    ...args: NonNullable<TableAction<T, K>["args"]>
  ): Promise<T[]> {
    return new Promise<T[]>((callback) => {
      switch (args[0]) {
        case "one":
          this.locks.next(
            new LazyPromise((records) =>
              thru(
                args,
                ([, id, predicate]) => (
                  predicate(this.selectById(records)(id)),
                  { actions: [], callback }
                ),
              ),
            ),
          );

          break;
        case "all":
          this.locks.next(
            new LazyPromise((records) =>
              thru(
                args,
                ([, predicate]) => (
                  predicate(records),
                  { actions: [], callback }
                ),
              ),
            ),
          );

          break;
        case "insert":
          this.locks.next(
            new LazyPromise(async (records) => {
              const [, predicate] = args;

              const newRecords = await predicate(records.map(this.selectId));
              const conflictingIds = intersectionBy(
                newRecords,
                records,
                this.selectId,
              );

              if (conflictingIds.length > 0) {
                throw new Error(`conflicting ids ${conflictingIds.join(", ")}`);
              }

              return {
                actions: [
                  {
                    id: v4(),
                    type: "insert",
                    payload: {
                      records: newRecords,
                    },
                  },
                ],
                callback,
              };
            }),
          );

          break;
        case "update":
          this.locks.next(
            new LazyPromise((records) =>
              thru(args, async ([, id, predicate]) => ({
                actions: [
                  {
                    id: v4(),
                    type: "update",
                    payload: {
                      record: await predicate(this.selectById(records)(id)),
                    },
                  },
                ],
                callback,
              })),
            ),
          );

          break;
        case "delete":
          this.locks.next(
            new LazyPromise(() =>
              thru(args, ([, id]) => ({
                actions: [{ id: v4(), type: "delete", payload: { id } }],
                callback,
              })),
            ),
          );
      }
    });
  }

  public async accessSome<A extends T[K][]>(
    [...ids]: [...A],
    spinner?: Ora,
  ): Promise<[...{ [K in keyof A]: T }]> {
    if (ids.length !== uniq(ids).length) {
      console.warn(`IDs ${ids.join(", ")} are not unique.`);
    }

    // @ts-expect-error
    return this.dispatch(
      "insert",
      (currentIds: T[K][]) =>
        // @ts-expect-error
        this.createSome(difference(uniq(ids), currentIds), spinner),
      // @ts-expect-error
    ).then((records) => ids.map(this.selectById(records)));
  }

  public async accessAll(): Promise<T[]> {
    return new Promise<T[]>((resolve) => this.dispatch("all", resolve));
  }
}
