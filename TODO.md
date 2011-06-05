## 71

 *  change Optimizer.explain so that it returns a SQLStatement object instead of a SQLStatementPlan one
 *  rename package `optimizers` to `optimizer`
 *  add method Optimizer.explain(sql, Configuration)
 *  move version check from constructor to `PGOptimizer.explain()`

## 75

 *  rename `SQLStatementPlan` to just `Plan`.

## 80: Open issue to use a logging library instead of edu.ucsc.dbtune.spi.core.Console

No need to roll our own. Also, we should define a policy of when and how to use logging to avoid polluting the code.

