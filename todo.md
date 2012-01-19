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

# 155 partition a workload in order to have one subset of statements per schema

# 156 Change the way BIP communicates with INUM optimizer
Change the implementation of BIP to comply with the new interface of INUM optimizer

# 157 Add class implements IndexInteractionFinder
Add a class of BIP that implements IndexInteractionFinder interface.

# 158 modify test for bipsolver subclasses

# 159 Fix DB2 metadata issues

# 160 Extend MetadataExtractorFunctionalTest

Add more workloads in order to check that a variety of kinds of catalogs can be extracted 
appropriately. At least all the TPC ones.

# 161 add support for multi-line in Workload

Add size() to Workload class.
