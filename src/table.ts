import { envConfig, nonNullable, nthArg } from "@cascateer/lib";
import { flatMap } from "@cascateer/lib/observable";
import { LazyPromise } from "@cascateer/lib/promise";
import { existsSync } from "fs";
import { mkdir, readdir, readFile, unlink, writeFile } from "fs/promises";
import {
  chunk,
  difference,
  fromPairs,
  Function1,
  intersectionBy,
  last,
  memoize,
  noop,
  tap,
  thru,
  uniq,
  without,
} from "lodash";
import objectHash from "object-hash";
import { Ora } from "ora";
import { resolve } from "path";
import {
  concatMap,
  identity,
  mergeAll,
  NextObserver,
  OperatorFunction,
  scan,
  startWith,
  Subject,
  Subscription,
} from "rxjs";
import { v4 } from "uuid";
import { File } from "./file";
import {
  FileTable,
  FileTableRecord,
  TableAction,
  TableActionCreator,
  TableActionCreatorResult,
  TableActionDispatchArgsUnion,
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

  trySelectById = (records: R[], id: R[K]): R | undefined =>
    tap(
      records.find((record) => this.selectId(record) === id),
      (record) => {
        if (record == null) {
          console.warn(`No record w/ id ${id} found in table ${this.id}`);
        }
      },
    );

  selectById = (records: R[], id: R[K]): R =>
    nonNullable(this.trySelectById(records, id));

  public async dispatch(
    ...args: TableActionDispatchArgsUnion<R, K>
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
                  console.warn(`conflicting ids ${conflictingIds.join(", ")}`);

                  return {
                    actions: [],
                    callback,
                  };
                }

                return {
                  actions: [
                    {
                      id: v4(),
                      type: "insert" as const,
                      payload: {
                        records: newRecords,
                      },
                    },
                  ].slice(0, Math.min(1, newRecords.length)),
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
                    : tap([], () => {
                        console.warn(
                          `record ${id} cannot be deleted, because it doesn't exist`,
                        );
                      }),
                  callback,
                }),
              ),
            ),
          );
      }
    });
  }

  public async getsertOne(id: R[K], spinner?: Ora): Promise<R> {
    return this.dispatch("insert", (currentIds: R[K][]) =>
      this.records(difference([id], currentIds), spinner),
    ).then((records) => this.selectById(records, id));
  }

  public async getsertMany(ids: R[K][], spinner?: Ora): Promise<R[]> {
    ids = uniq(ids);

    return this.dispatch("insert", (currentIds: R[K][]) =>
      this.records(difference(uniq(ids), currentIds), spinner),
    ).then((records) => ids.map((id) => this.selectById(records, id)));
  }

  public async getAll(): Promise<R[]> {
    return new Promise<R[]>((resolve) => this.dispatch("all", resolve));
  }

  reduceActions =
    <R, K extends keyof R>(
      transform: (records: R[], ...actions: TableAction<R, K>[]) => R[],
      seed: LazyPromise<
        R[],
        {
          actions: TableAction<R, K>[];
          callback?: Function1<R[], void>;
        }
      >,
    ): OperatorFunction<TableActionCreator<R, K>, TableAction<R, K>> =>
    (source) =>
      source.pipe(
        startWith(seed),
        scan(
          (result, actions) =>
            result.then(({ records, previousAction }) =>
              actions.start(records).then(({ actions, callback }) => {
                const transformedRecords = tap(
                  transform(records, ...actions),
                  callback ?? noop,
                );

                return thru(
                  previousAction == null && actions.length > 0
                    ? chunk(transformedRecords, 40).reduce(
                        (actions, records, id) =>
                          actions.concat({
                            id: `${id}`,
                            previousId: last(actions)?.id,
                            type: "insert",
                            payload: {
                              records,
                            },
                          }),
                        new Array<TableAction<R, K>>(),
                      )
                    : actions.map((action, actionIndex, actions) => {
                        const previousId =
                          actions[actionIndex - 1]?.id ?? previousAction!.id;

                        return {
                          ...action,
                          id: `${+previousId + 1}`,
                          previousId,
                        };
                      }),
                  (actions) => ({
                    records: transformedRecords,
                    actions,
                    previousAction: last(actions) ?? previousAction,
                  }),
                );
              }),
            ),
          Promise.resolve<{
            records: Array<R>;
            actions: Array<TableAction<R, K>>;
            previousAction?: TableAction<R, K>;
          }>({
            records: [],
            actions: [],
          }),
        ),
        mergeAll(),
        concatMap(async ({ actions }) => {
          for (const action of actions) {
            if (action.previousId == null) {
              const files = await readdir(this.path);

              for (const file of files) {
                await unlink(resolve(this.path, file));
              }
            }

            const path = resolve(
              this.path,
              `A${action.id.padStart(6, "0")}.json`,
            );

            await writeFile(path, JSON.stringify(action, null, "\t"));
          }

          return actions;
        }),
        flatMap(identity),
      );
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
          .pipe(this.reduceActions(this.applyActions, this.readActions))
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
      this.getsertOne(url, spinner).then(({ name, checksum }) =>
        new File(name).verified(checksum),
      );
  };
