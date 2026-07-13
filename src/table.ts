import { envConfig, findDupeBy, nonNullable, nthArg } from "@cascateer/lib";
import { LazyPromise } from "@cascateer/lib/promise";
import assert from "assert";
import { existsSync } from "fs";
import { mkdir, readdir, readFile, unlink, writeFile } from "fs/promises";
import {
  difference,
  fromPairs,
  intersectionBy,
  memoize,
  thru,
  uniq,
  without,
} from "lodash";
import objectHash from "object-hash";
import { Ora } from "ora";
import { resolve } from "path";
import { concatMap, NextObserver, Subject, Subscription } from "rxjs";
import { v4 } from "uuid";
import { File } from "./file";
import { reduceActions } from "./observables/reduceActions";
import {
  FileTable,
  FileTableRecord,
  TableAction,
  TableActionCreator,
  TableActionCreatorResult,
  TableRecordCreator,
} from "./types";

const { DATABASE_TABLE_BASE_URL = "tables" } = envConfig();

export class Table<R, K extends keyof R> {
  private static readonly BASE_URL = DATABASE_TABLE_BASE_URL;

  constructor(
    public id: string,
    public key: K,
    public records: TableRecordCreator<R, K>,
    private observer: NextObserver<TableActionCreator<R, K>>,
  ) {}

  get path() {
    return resolve(Table.BASE_URL, this.id);
  }

  protected readonly readActions = new LazyPromise<
    R[],
    TableActionCreatorResult<R, K>
  >(async () => {
    if (!existsSync(this.path)) {
      await mkdir(this.path, { recursive: true });
    }

    const actions = new Array<TableAction<R, K>>();

    for (const file of await readdir(this.path)) {
      actions.push(
        await readFile(resolve(this.path, file), "utf-8").then<
          TableAction<R, K>
        >(JSON.parse),
      );
    }

    const actionsMap = fromPairs(
      actions.map((action) => [action.previousId ?? "", action]),
    );

    const newActions = new Array<TableAction<R, K>>();
    let action = actionsMap[""];

    while (action != null) {
      newActions.push(action);

      action = actionsMap[action.id];
    }

    return {
      actions: newActions,
    };
  });

  applyActions = (records: R[], ...actions: TableAction<R, K>[]) =>
    actions.reduce((records, action) => {
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
          return without(records, this.selectById(records, action.payload.id));
      }

      return records;
    }, records);

  selectId = (record: R): R[K] => record[this.key];
  selectById = (records: R[], id: R[K]): R => (
    assert(findDupeBy(records, this.selectId) == null),
    thru(
      records.find((record) => this.selectId(record) === id),
      (record) => {
        if (record == null) {
          console.error(`No record w/ id ${id} found in table ${this.id}`);
        }

        return nonNullable(record);
      },
    )
  );

  public async dispatch(
    ...args: NonNullable<TableAction<R, K>["args"]>
  ): Promise<R[]> {
    return new Promise<R[]>((callback) => {
      switch (args[0]) {
        case "one":
          this.observer.next(
            new LazyPromise(
              (records): TableActionCreatorResult<R, K> =>
                thru(
                  args,
                  ([, id, predicate]) => (
                    predicate(this.selectById(records, id)),
                    {
                      actions: [],
                      callback,
                    }
                  ),
                ),
            ),
          );

          break;
        case "all":
          this.observer.next(
            new LazyPromise(
              (records): TableActionCreatorResult<R, K> =>
                thru(
                  args,
                  ([, predicate]) => (
                    predicate(records),
                    {
                      actions: [],
                      callback,
                    }
                  ),
                ),
            ),
          );

          break;
        case "insert":
          this.observer.next(
            new LazyPromise(
              async (records): Promise<TableActionCreatorResult<R, K>> => {
                const [, predicate] = args;

                const newRecords = await predicate(records.map(this.selectId));
                const conflictingIds = intersectionBy(
                  newRecords,
                  records,
                  this.selectId,
                );

                if (conflictingIds.length > 0) {
                  throw new Error(
                    `conflicting ids ${conflictingIds.join(", ")}`,
                  );
                }

                return {
                  actions:
                    newRecords.length > 0
                      ? [
                          {
                            id: v4(),
                            type: "insert",
                            payload: {
                              records: newRecords,
                            },
                          },
                        ]
                      : [],
                  callback,
                };
              },
            ),
          );

          break;
        case "update":
          this.observer.next(
            new LazyPromise((records) =>
              thru(
                args,
                async ([, id, predicate]): Promise<
                  TableActionCreatorResult<R, K>
                > => {
                  const targetRecord = this.selectById(records, id);
                  const updatedTargetRecord = await predicate(targetRecord);

                  return {
                    actions:
                      objectHash(targetRecord ?? null) !==
                      objectHash(updatedTargetRecord ?? null)
                        ? [
                            {
                              id: v4(),
                              type: "update",
                              payload: {
                                record: updatedTargetRecord,
                              },
                            },
                          ]
                        : [],
                    callback,
                  };
                },
              ),
            ),
          );

          break;
        case "delete":
          this.observer.next(
            new LazyPromise((records) =>
              thru(
                args,
                ([, id]): TableActionCreatorResult<R, K> => ({
                  actions: records.some(
                    (record) => this.selectId(record) === id,
                  )
                    ? [
                        {
                          id: v4(),
                          type: "delete",
                          payload: {
                            id,
                          },
                        },
                      ]
                    : [],
                  callback,
                }),
              ),
            ),
          );
      }
    });
  }

  public async accessSome<A extends R[K][]>(
    [...ids]: [...A],
    spinner?: Ora,
  ): Promise<[...{ [K in keyof A]: R }]> {
    if (ids.length !== uniq(ids).length) {
      console.warn(`IDs ${ids.join(", ")} are not unique.`);
    }

    // @ts-expect-error
    return this.dispatch(
      "insert",
      (currentIds: R[K][]) =>
        // @ts-expect-error

        this.records(difference(uniq(ids), currentIds), spinner),
      // @ts-expect-error
    ).then((records) => ids.map((id) => this.selectById(records, id)));
  }

  public async accessAll(): Promise<R[]> {
    return new Promise<R[]>((resolve) => this.dispatch("all", resolve));
  }
}

export const createTable = memoize(
  <R, K extends keyof R>(
    id: string,
    key: K,
    records: TableRecordCreator<R, K>,
  ) =>
    class TableInstance extends Table<R, K> {
      private static readonly actionsSubject = new Subject<
        TableActionCreator<R, K>
      >();
      private static actionsSubscription?: Subscription;

      constructor() {
        super(id, key, records, TableInstance.actionsSubject);

        TableInstance.actionsSubscription ??= TableInstance.actionsSubject
          .pipe(
            reduceActions(this.applyActions, this.readActions),
            concatMap(async (action, actionIndex) => {
              console.log(id, action.id, action.previousId);

              if (action.previousId == null) {
                const files = await readdir(this.path);

                console.log(id + "\n\t" + files.join("\n\t"));

                for (const file of files) {
                  await unlink(resolve(this.path, file));
                }
              }

              const path = resolve(
                this.path,
                `A${actionIndex.toString().padStart(6, "0")}-${action.id}.json`,
              );

              await writeFile(path, JSON.stringify(action, null, "\t"));

              return action;
            }),
          )
          .subscribe();
      }
    },
  nthArg(0),
);

export const createFileTable = (
  id: string,
  records: TableRecordCreator<FileTableRecord, "url">,
) =>
  class
    extends createTable<FileTableRecord, "url">(id, "url", records)
    implements FileTable
  {
    getFile = (url: string, spinner?: Ora) =>
      this.accessSome([url], spinner).then(([{ name, checksum }]) =>
        new File(name).verified(checksum),
      );
  };
