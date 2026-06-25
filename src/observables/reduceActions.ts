import { property } from "@cascateer/lib";
import { flatMap } from "@cascateer/lib/observables";
import { LazyPromise } from "@cascateer/lib/promises";
import { Function1, noop, tap } from "lodash";
import { mergeAll, OperatorFunction, scan, startWith } from "rxjs";
import { TableAction } from "../table";

export interface TableActionCreatorResult<T, K extends keyof T> {
  actions: TableAction<T, K>[];
  callback?: Function1<T[], void>;
}

export interface TableActionCreator<T, K extends keyof T> extends LazyPromise<
  T[],
  TableActionCreatorResult<T, K>
> {}

export const reduceActions =
  <T, K extends keyof T>(
    transform: (state: T[], ...actions: TableAction<T, K>[]) => T[],
    seed: LazyPromise<
      T[],
      {
        actions: TableAction<T, K>[];
        callback?: Function1<T[], void>;
      }
    >,
  ): OperatorFunction<TableActionCreator<T, K>, TableAction<T, K>> =>
  (source) =>
    source.pipe(
      startWith(seed),
      scan(
        (result, actions) =>
          result.then(({ state }) =>
            actions.run(state).then(({ actions, callback }) => ({
              state: tap(transform(state, ...actions), callback ?? noop),
              actions,
            })),
          ),
        Promise.resolve({
          state: new Array<T>(),
          actions: new Array<TableAction<T, K>>(),
        }),
      ),
      mergeAll(),
      flatMap(property("actions")),
      scan(
        ({ previousAction }, action) => ({
          previousAction: { ...action, previousId: previousAction?.id },
        }),
        <{ previousAction?: TableAction<T, K> }>{},
      ),
      flatMap(({ previousAction }) => previousAction ?? []),
    );
