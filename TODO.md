## 70: Use a logging library instead of edu.ucsc.dbtune.spi.core.Console

No need to roll our own. Also, we should define a policy of when and how to use logging to avoid polluting the code.

eg. //TODO insert ugly code

## 71: Convert SQLTypes to an Enum

Taken from comments done by Huascar on class `dbtune.core.metadata.SQLTypes`

## 72:

## 73:

## 74:

## 75: Refactor SQLStatement to represent optimized queries.

The goal is to merge `dbtune.core.SQLStatement`, `dbtune.core.ExplainInfo` and `dbtune.advisor.ProfiledQuery` into a single 
`dbtune.core.optimizers.SQLStatement` class.

TODO:
 *  rename `SQLStatementPlan` to just `Plan`.

## [FIXED] 76: Make SQLStatement return the tables and indexes it's touching.

This will allow to easily obtain the set of indexes that are referred by the plan of an optimized statement.

## 77: Create Predicate class

This should be preceded by a discussion on whether or not is useful to abstract predicates. Related to issue #72 in some 
sense: if predicates are abstracted then `Operator` can refer to `Predicate` objects.

## 78: Make SQLStatement return the columns it's touching.

Similarly to issue #71, an operator also refers to predicates. If issue #71 is implemented, then we can bind operators to 
`Predicate` objects. If not, then we just add references to `Column` objects.

## 79: Obtain plan diffs with respect to configurations

Given two `SQLStatementPlan` objects, obtain the differences between them in terms of the indexes they're using.

## 80: Consolidate optimizer interfaces

This will integrate all the (what-if) optimization interfaces:

 *  `core.AbstractIBGWhatIfOptimizer`
 *  `core.AbstractWhatIfOptimizer`
 *  `core.IBGWhatIfOptimizer`
 *  `core.WhatIfOptimizationBuilderImpl`
 *  `core.WhatIfOptimizerFactory`
 *  `core.WhatIfOptimizer`

and refactor the implementations:

 *  `core.PostgresIBGWhatIfOptimizer`
 *  `core.PostgresWhatIfOptimizer`
 *  `core.DB2IBGWhatIfOptimizer`
 *  `core.DB2WhatIfOptimizer`

TODO:
 *  change Optimizer.explain so that it returns a SQLStatement object instead of a SQLStatementPlan one
 *  rename package `optimizers` to `optimizer`
 *  add method Optimizer.explain(sql, Configuration)
 *  move version check from constructor to `PGOptimizer.explain()`

## 81: Adopt DBCP to handle JDBC connections

Instead of rolling our own connection management code we should use [DBCP](http://commons.apache.org/dbcp/). With this change 
`java.sql.Connection` becomes the main connectivity class and we'll be able to drop all the following:

 *  `core.DatabaseSystem` (the current version)
 *  `core.DatabaseSession`
 *  `core.ConnectionManager`
 *  `core.AbstractConnectionManager`
 *  `core.AbstractDatabaseConnection`
 *  `core.AbstractDatabaseSession`
 *  `core.JdbcConnectionFactoryImpl`
 *  `core.JdbcConnectionFactory`
 *  `core.JdbcConnection`
 *  `core.JdbcConnectionManager`
 *  `core.Platform`

## 82: Move contents of `dbtune.core` to `dbtune`

The core functionality is in the main `src` project and extensions in `extenstions` folder. Consequently, there's no need to 
explicitly have a `core` package.



