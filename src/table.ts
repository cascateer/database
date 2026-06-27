import { findDupeBy, nonNullable, nthArg } from "@cascateer/lib";
import { LazyPromise } from "@cascateer/lib/promises";
import assert from "assert";
import { existsSync } from "fs";
import { mkdir, readdir, readFile, writeFile } from "fs/promises";
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
import { mergeMap, NextObserver, Subject, Subscription } from "rxjs";
import { v4 } from "uuid";
import { reduceActions } from "./observables/reduceActions";
import {
  TableAction,
  TableActionCreator,
  TableActionCreatorResult,
  TableRecordCreator,
} from "./types";

class Table<R, K extends keyof R> {
  private static readonly BASE_URL = resolve(__dirname, "../tables");

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

    const files = await readdir(this.path);
    const actions = new Array<TableAction<R, K>>();

    for (const file of files) {
      actions.push(
        await readFile(resolve(this.path, file), "utf-8").then<
          TableAction<R, K>
        >(JSON.parse),
      );
    }

    const actionsMap = fromPairs(
      actions.map((action) => [action.previousId ?? "", [action]]),
    );

    return {
      actions: actions.reduce(
        (actions, action) => actions.concat(actionsMap[action.id] ?? []),
        actionsMap[""] ?? [],
      ),
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
          return without(records, this.selectById(records)(action.payload.id));
      }

      return records;
    }, records);

  selectId = (record: R): R[K] => record[this.key];
  selectById =
    (records: R[]) =>
    (id: R[K]): R => (
      assert(findDupeBy(records, this.selectId) == null),
      nonNullable(records.find((record) => this.selectId(record) === id))
    );

  public async dispatch(
    ...args: NonNullable<TableAction<R, K>["args"]>
  ): Promise<R[]> {
    return new Promise<R[]>((callback) => {
      switch (args[0]) {
        case "one":
          this.observer.next(
            new LazyPromise((records) =>
              thru(
                args,
                ([, id, predicate]) => (
                  predicate(this.selectById(records)(id)),
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
            new LazyPromise((records) =>
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
            }),
          );

          break;
        case "update":
          this.observer.next(
            new LazyPromise((records) =>
              thru(args, async ([, id, predicate]) => {
                const targetRecord = this.selectById(records)(id);
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
              }),
            ),
          );

          break;
        case "delete":
          this.observer.next(
            new LazyPromise((records) =>
              thru(args, ([, id]) => ({
                actions: records.some((record) => this.selectId(record) === id)
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
              })),
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
    ).then((records) => ids.map(this.selectById(records)));
  }

  public async accessAll(): Promise<R[]> {
    return new Promise<R[]>((resolve) => this.dispatch("all", resolve));
  }
}

export { type Table };

export const createTable = memoize(
  <R, K extends keyof R>(
    id: string,
    key: K,
    records: TableRecordCreator<R, K>,
  ) =>
    class TableInstance extends Table<R, K> {
      private static readonly actions = new Subject<TableActionCreator<R, K>>();
      private static actionsSubscription?: Subscription;

      constructor() {
        super(id, key, records, TableInstance.actions);

        TableInstance.actionsSubscription ??= TableInstance.actions
          .pipe(
            reduceActions(this.applyActions, this.readActions),
            mergeMap(async (action) => {
              const path = resolve(this.path, `${action.id}.json`);

              if (!existsSync(path)) {
                await writeFile(path, JSON.stringify(action));
              }

              return action;
            }),
          )
          .subscribe();
      }
    },
  nthArg(0),
);
