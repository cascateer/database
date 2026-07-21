import { property } from "@cascateer/lib";
import { flatMap } from "@cascateer/lib/observable";
import { LazyPromise } from "@cascateer/lib/promise";
import { chunk, Function1, last, noop, tap, thru } from "lodash";
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
      flatMap(property("actions")),
    );
