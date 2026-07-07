import { property } from "@cascateer/lib";
import { flatMap } from "@cascateer/lib/observable";
import { LazyPromise } from "@cascateer/lib/promise";
import { Function1, last, noop, tap } from "lodash";
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
          result.then(({ records, previousActionId }) =>
            actions.start(records).then(({ actions, callback }) => ({
              records: tap(transform(records, ...actions), callback ?? noop),
              actions: actions.map((action, actionIndex, actions) => ({
                ...action,
                previousId: actions[actionIndex - 1]?.id ?? previousActionId,
              })),
              previousActionId: last(actions)?.id ?? previousActionId,
            })),
          ),
        Promise.resolve<{
          records: Array<R>;
          actions: Array<TableAction<R, K>>;
          previousActionId?: string;
        }>({
          records: [],
          actions: [],
        }),
      ),
      mergeAll(),
      flatMap(property("actions")),
    );
