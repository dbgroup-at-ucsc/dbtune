# Tasks per issue

## 20

No need to roll our own. Also, we should define a policy of when and how to use logging to avoid polluting the code.

## 43

 *  create `connectivity` package
 *  move all Connection classes into it
 *  create `Commands` class/interface derive `PGCommands` and `DB2Commands` from it. (??? better to drop these and let others 
    worry about SQL generation, like PGOptimizer, PGMetaDataExtractor, etc...)

## 44

 * extend `Column` with stuff used in `PGColumn` and `DB2Column`
 * replace `PGColumn` and `DB2Column` by `Column`
 * drop `ReifiedTypes` by using a regular `ArrayList<Index>`.
 * translate all the method names from `DBIndex` to `Index`-lingo in `PGIndex` and `DB2Index`
 * add remaining types to `Index` from `DB2Index` and `PGIndex`: `BLOCK,DIMENSION,REGULAR`
 * add new field to `Index` to represent `scanOption`: `REVERSIBLE,NON_REVERSIBLE,SYNCHRONIZED`
 * remove `I extends DBIndex` noise: `egrep -iR '.*extends DBIndex' src/edu/ucsc/dbtune/`
 * make `NewPGIndex` and `NewDB2Index` by make `PGIndex` and `DB2Index` derive from `Index` instead of `DBIndex`
 * check that `NewPGIndex` and `NewDB2Index` don't replicate `Index` and that the actual platform-dependent code (like 
   `getCreateStatement()`) is contained in them.
 * replace `PGIndex` by `NewPGIndex`
 * replace `DB2Index` by `NewDB2Index`
 * drop `AbstractIndex`,`DBIndex`
 * fix issue #74
 * more test!!
 * commit :)

## 46

 *  rename package `optimizers` to `optimizer`
 *  move WhatIf classes from core.\* into `optimizer`
 *  move ExplainInfo and implementations into `optimizer`

## 53

 *  make metadata retrieval part of the `DatabaseConnection` initilization protocol. This will help in fullfilling the 
    [DBTune use case](https://github.com/dbgroup-at-ucsc/dbtune/wiki/java-packages-structure).
 *  in `Table` class, drop constructor:
 
    ```java
    Table(String dbName, String schemaName, String name) {
    ```
 
    the schema and db names should be retrieved from the `Schema` and `Catalog` classes respectively. This info should be 
    available upon connection as part of the metadata extraction mechanism. Also drop fields `schemaName` and `dbName`; 
    methods `hashCode()` and `equals()`.

## 71

 *  change Optimizer.explain so that it returns a SQLStatement object instead of a `SQLStatementPlan` one
 *  add method Optimizer.explain(sql, Configuration)
 *  move version check from constructor to `PGOptimizer.explain()`

## 72

 *  take a look at Querulous' [`ApachePoolingDatabase`][querulous_dbcp] class to get an idea on how to implement this

## 74

 *  add more methods to `IndexTest`; create `PGIndexTest` and `DB2IndexTest` (testing `NewDB2Index` and `NewPGIndex`) classes 
    to test use cases from everywhere `DBIndex`, `DB2Index` and `PGIndex` is used (mainly `PGCommands` and `DB2Commands`)
 *  test stuff assumed in the CLI
 *  think of any other based on our intuition: how is metadata gonna be used besides the above?

## 75

 *  rename `SQLStatementPlan` to just `Plan`.

## 80

d

## 84:

d

## 91:

 *  fix #44
 *  connectivity:
     * add mysql driver
     * modify Platform.java to include MySQL
 *  integrate metadata stuff:
     * create `MySQLIndex` and `MySQLIndexTest` classes
     * add MySQL metadata extractor
 *  create MySQLWhatIfOptimizer class
 *  run existing Wfit and tests against MySQL

## 92:

 *  create `DBSystem` that only implements the `newIndex()` method

# refs

[querulous_dbcp]: https://github.com/twitter/querulous/blob/master/src/main/scala/com/twitter/querulous/database/ApachePoolingDatabase.scala
