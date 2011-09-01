# Tasks per issue

## 20

No need to roll our own. Also, we should define a policy of when and how to use logging to avoid polluting the code.

## 52

Things that the current test doesn't check:

 *  foreign keys constraints
 *  unique constraints
 *  not null constraints
 *  default constraints
 *  index cardinality
 *  `getByte()` for each db object
 *  number of pages

## 64

Define the following dbms-agnostic interface

```
public Index createIndex(List<Column>, List<Boolean>, boolean, boolean);
```

## 72

 *  take a look at Querulous' [`ApachePoolingDatabase`][querulous_dbcp] class to get an idea on how to implement this

## 74

 *  add more methods to `IndexTest`; create `PGIndexTest` and `DB2IndexTest` classes to test use cases from everywhere 
    `DBIndex`, `DB2Index` and `PGIndex` is used (mainly `PGCommands` and `DB2Commands`)
 *  test stuff assumed in the CLI
 *  think of any other based on our intuition: how is metadata gonna be used besides the above?

## 101

 *  fix #52
 *  fix #74
 *  fix #64
 *  implement explain(SQLStatement)
 *  implement explain(SQLStatement,Configuration)
    *  fix #3, #4 and #5 of mysqlpp project
 *  run existing Wfit and tests against MySQL

# refs

[querulous_dbcp]: https://github.com/twitter/querulous/blob/master/src/main/scala/com/twitter/querulous/database/ApachePoolingDatabase.scala
