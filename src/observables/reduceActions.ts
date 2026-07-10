import { property } from "@cascateer/lib";
import { flatMap } from "@cascateer/lib/observable";
import { LazyPromise } from "@cascateer/lib/promise";
import { Function1, last, noop, tap, thru } from "lodash";
import { mergeAll, OperatorFunction, scan, startWith } from "rxjs";
import { TableAction, TableActionCreator } from "../types";

export const reduceActions =
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
                previousAction == null && 0 in actions
                  ? [
                      <TableAction<R, K>>{
                        id: actions[0].id,
                        type: "insert",
                        payload: {
                          records: transformedRecords,
                        },
                      },
                    ]
                  : actions.map((action, actionIndex, actions) => ({
                      ...action,
                      previousId:
                        actions[actionIndex - 1]?.id ?? previousAction?.id,
                    })),
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
      flatMap(property("actions")),
    );
