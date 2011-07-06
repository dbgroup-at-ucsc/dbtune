# Tasks per issue

## 20

No need to roll our own. Also, we should define a policy of when and how to use logging to avoid polluting the code.

## 28

This is needed in order to solve #44. In the case of DB2, we need to merge also DB2IndexMetadata

## 43

 *  create `connectivity` package
 *  create `Commands` class/interface
 *  derive `PGCommands` and `DB2Commands` from it.

## 44

 * fix issue #74
 * fix issue #28
 * remove `I extends DBIndex` noise: `egrep -iR '.*extends DBIndex' src/edu/ucsc/dbtune/`
 * create `DBSystem` that only implements the `newIndex()` method (the commit for this should refer its own issue)
 * replace `PGTable` by `Table` (may be in its own issue)
 * replace `PGColumn` and `DB2Column` by `Column` (may need to open an issue)
 * drop ReifiedTypes by using a regular `ArrayList<Index>`.
 * drop IndexDescriptor
 * add remaining types to `Index` from `DB2Index` and `PGIndex`: `BLOCK,DIMENSION,REGULAR`
 * add new field to `Index` to represent `scanOption`: `REVERSIBLE,NON_REVERSIBLE,SYNCHRONIZED`
 * refactor `PGIndex` and `DB2Index` by removing everything that is already in `Index` and then just add the actual 
   platform-dependent code (like `getCreateStatement()`). Make them derive from `Index` instead of `DBIndex`
 * replace `DBIndex`, `*IndexSchema` and `DB2IndexMetadata` by `Index` (this will trigger all the changes on everything that 
   touches these three classes).
 * drop `DBIndex`, `*IndexSchema` and `DB2IndexMetadata`
 * test!!
 * commit :)

## 71

 *  change Optimizer.explain so that it returns a SQLStatement object instead of a `SQLStatementPlan` one
 *  rename package `optimizers` to `optimizer`
 *  add method Optimizer.explain(sql, Configuration)
 *  move version check from constructor to `PGOptimizer.explain()`

## 72

 *  take a look at Querulous' [`ApachePoolingDatabase`][querulous_dbcp] class to get an idea on how to implement this

## 74

 *  write tests for everything assumed in `MetaDataExtractor`
 *  find use cases from everywhere `DBIndex` and `DBIndexSchema` is used (mainly `PGCommands` and `DB2Commands`)
 *  test stuff assumed in the CLI
 *  think of any other based on our intuition: how is metadata gonna be used besides the above?

## 75

 *  rename `SQLStatementPlan` to just `Plan`.

## 80

## 83

dbtune.workload package

## 84:

[querulous_dbcp]: https://github.com/twitter/querulous/blob/master/src/main/scala/com/twitter/querulous/database/ApachePoolingDatabase.scala
