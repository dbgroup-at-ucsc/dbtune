# Tasks per issue

## 20

No need to roll our own. Also, we should define a policy of when and how to use logging to avoid polluting the code.

## 52

Things that the current test doesn't check:

 *  check base configuration (materialized set)
 *  primary keys
 *  foreign keys constraints
 *  unique constraints
 *  not null constraints
 *  default constraints
 *  index cardinality
 *  `getByte()` for each db object
 *  number of pages

## 74

 *  test stuff assumed in the CLI
 *  think of any other based on our intuition: how is metadata gonna be used besides the above?

# refs

[querulous_dbcp]: https://github.com/twitter/querulous/blob/master/src/main/scala/com/twitter/querulous/database/ApachePoolingDatabase.scala
