# 151 Extend classes from dbtune.optimizer for INUM usage
    
 * specifically, add a Tree.leafs() method to obtain the operators that access the base tables in a 
 plan.

 * add a ExplainedSQLStatement.getPlan() method that returns the execution plan

 * add a SQLStatementPlan.getReferencedTables() method that returns the set of tables that are 
 referenced by a statement plan

 * add tests for all the above
 
# 152 Add an Inum-specific execution plan information

Add a `TableAccessSlot` class that derives from `Operator`

Add an InumPlan extend `SQLStatementPlan` class and add inum-specific functions like:

 * double getInternalCost()
 * double plugIntoSlots(Set<Index> atomicConfiguration)
 * List<TableAccessSlot> getAccessSlots()

Add an IndexFullTableScan class that corresponds to the FTS of a base table

# 153 Refactor MatchingStrategy and implementations

 * refactor the MatchingStrategy interface
 * implement a ExhaustiveMatchingStrategy class
 * implement a GreedyMatchingStrategy class

# 154 Create InumSpaceComputation interface and implementations

Add an EagerSpaceComputation class that obtains the whole template plan space