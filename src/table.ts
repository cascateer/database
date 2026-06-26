import { findDupeBy, nonNullable } from "@cascateer/lib";
import { LazyPromise } from "@cascateer/lib/promises";
import assert from "assert";
import { existsSync } from "fs";
import { readdir, readFile, writeFile } from "fs/promises";
import { difference, intersectionBy, thru, uniq, without } from "lodash";
import { Ora } from "ora";
import { resolve } from "path";
import { mergeMap } from "rxjs";
import { v4 } from "uuid";
import { reduceActions } from "./observables/reduceActions";
import { TableAction, TableActionCreatorResult, Tables } from "./types";

type R<t extends keyof Tables> = Tables[t]["record"];
type K<t extends keyof Tables> = Tables[t]["key"];

export class Table<t extends keyof Tables> {
  private static readonly BASE_URL = resolve(__dirname, "../tables");

  private static readonly tables: Tables;

  applyActions(records: R<t>[], ...actions: TableAction<R<t>, K<t>>[]) {
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

  get key(): K<t> {
    return Table.tables[this.id].key;
  }

  get records() {
    return Table.tables[this.id].records;
  }

  get actions() {
    return nonNullable(Table.tables[this.id].actions);
  }

  constructor(public id: t) {
    this.actions
      .pipe(
        reduceActions(this.applyActions, this.readActions),
        mergeMap(async (action) => {
          const path = resolve(this.path, `${action.id}.json`);

          if (!existsSync(path)) {
            return writeFile(path, JSON.stringify(action));
          }
        }),
      )
      .subscribe();
  }

  get path() {
    return resolve(Table.BASE_URL, this.id);
  }

  private readonly readActions = new LazyPromise<
    R<t>[],
    TableActionCreatorResult<R<t>, K<t>>
  >(
    () =>
      new Promise((callback) =>
        readdir(this.path).then(async (files) => {
          const actions = new Array<TableAction<R<t>, K<t>>>();

          for (const file of files) {
            actions.push(
              await readFile(resolve(this.path, file), "utf-8").then<
                TableAction<R<t>, K<t>>
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

  selectId = (record: R<t>): R<t>[K<t>] => record[this.key];
  selectById =
    (records: R<t>[]) =>
    (id: R<t>[K<t>]): R<t> => (
      assert(findDupeBy(records, this.selectId) == null),
      nonNullable(records.find((record) => this.selectId(record) === id))
    );

  public async dispatch(
    ...args: NonNullable<TableAction<R<t>, K<t>>["args"]>
  ): Promise<R<t>[]> {
    return new Promise<R<t>[]>((callback) => {
      switch (args[0]) {
        case "one":
          this.actions.next(
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
          this.actions.next(
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
          this.actions.next(
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
          this.actions.next(
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
          this.actions.next(
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

  public async accessSome<A extends R<t>[K<t>][]>(
    [...ids]: [...A],
    spinner?: Ora,
  ): Promise<[...{ [K in keyof A]: R<t> }]> {
    if (ids.length !== uniq(ids).length) {
      console.warn(`IDs ${ids.join(", ")} are not unique.`);
    }

    // @ts-expect-error
    return this.dispatch("insert", (currentIds: R<t>[K<t>][]) =>
      this.records(difference(uniq(ids), currentIds), spinner),
    ).then((records) => ids.map(this.selectById(records)));
  }

  public async accessAll(): Promise<R<t>[]> {
    return new Promise<R<t>[]>((resolve) => this.dispatch("all", resolve));
  }
}
