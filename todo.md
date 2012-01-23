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

Add an IndexFullTableScan singleton-per-table class that corresponds to the FTS of a base table. 

Add an Index.isCoveredBy()

# 153 Refactor MatchingStrategy and implementations

 * refactor the MatchingStrategy interface
 * implement a ExhaustiveMatchingStrategy class
 * implement a GreedyMatchingStrategy class

# 154 Create InumSpaceComputation interface and implementations

Add an EagerSpaceComputation class that obtains the whole template plan space

# 155 partition a workload in order to have one subset of statements per schema

In the meantime we'll need to have to find a workaround, because for this we need to parse and bind 
and since it's done at an early stage of the recommendation process, this might be tricky. I think 
we're find assuming that only one workload is received

# 156 Change the way BIP communicates with INUM optimizer
Change the implementation of BIP to comply with the new interface of INUM optimizer

# 157 Add class implements IndexInteractionFinder
Add a class of BIP that implements IndexInteractionFinder interface.

# 158 modify test for bipsolver subclasses

# 159 Extend MetadataExtractorFunctionalTest

Add more workloads in order to check that a variety of kinds of catalogs can be extracted 
appropriately. At least all the TPC ones.

# 160 Fix DB2 metadata issues

No issues found. It looks like other empty databases are getting in the way.

# 161 add support for multi-line in Workload

Add size() to Workload class.

# 162 replace the mapping of schema to workload by workload only

Replace a part of the input to the BIP to consist of only workload, instead of 
workload that is partitioned in terms of the schema on which the queries are defined on.

# 163 DB2Optimizer bug

The contents of columns corresponding to system tables need to be trimmed.

# 164 FullTableScanIndex bug

The constructor of FTS should contain information to link to the table on which this object is 
defined. In addition, the name of FTS should include the table name in order to uniquely identify
a FTS for a table.

# 165 Add LogListener class to record the running time of BIP

Implementating a logger that records the running time of BIP

# 166 Create an InterestingOrder class

The Index class is not usable since every time that an index is created, it is logically added to 
the corresponding schema where it belongs. We need another entity that can "point" to column sets 
without needing to apply the containment logic of the dbtune.metadata package.

This can be easily added by extending the Index class.

