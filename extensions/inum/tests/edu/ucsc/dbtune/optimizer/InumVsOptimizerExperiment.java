package edu.ucsc.dbtune.optimizer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Sets.cartesianProduct;
import static com.google.common.collect.Sets.powerSet;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.optimizer.plan.Operator.INDEX_AND;
import static edu.ucsc.dbtune.optimizer.plan.Operator.INDEX_OR;
import static edu.ucsc.dbtune.optimizer.plan.Operator.NESTED_LOOP_JOIN;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesReferencingTable;
import static edu.ucsc.dbtune.util.MetadataUtils.getNumberOfDistinctIndexesByContent;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
import static edu.ucsc.dbtune.util.TestUtils.workloads;

/**
 * @author Ivo Jimenez
 */
public final class InumVsOptimizerExperiment
{
    private static DatabaseSystem db;
    private static Environment env;
    private static Optimizer optimizer;
    private static Optimizer delegate;
    private static CandidateGenerator candGen;
    private static final Random RANDOM = new Random(System.currentTimeMillis());

    /**
     * utility class.
     */
    private InumVsOptimizerExperiment()
    {
    }

    /**
     * @throws Exception
     *      if {@link #newDatabaseSystem} throws an exception
     */
    public static void beforeClass() throws Exception
    {
        env = Environment.getInstance();
        db = newDatabaseSystem(env);
        optimizer = db.getOptimizer();
        delegate = getBaseOptimizer(optimizer);
        candGen = CandidateGenerator.Factory.newCandidateGenerator(env, delegate);
        
        loadWorkloads(db.getConnection());
    }

    /**
     * @throws Exception
     *      if something goes wrong while closing the connection to the dbms
     */
    public static void afterClass() throws Exception
    {
        db.getConnection().close();
    }

    /**
     * run.
     *
     * @param args
     *      arguments
     * @throws Exception
     *      if something goes wrong
     */
    public static void main(String[] args) throws Exception
    {
        beforeClass();

        if (delegate == null) throw new Exception("No delegate");

        int i = 1;
        int j = 0;
        int stopCount = 50;
        int prepareWhatIfCount = 0;
        int explainWhatIfCount = 0;
        int totalWhatIfCount = 0;
        long time;
        long prepareTime;
        long explainTimePrepared;
        long explainTimeExplained;
        ExplainedSQLStatement prepared;
        ExplainedSQLStatement explained;
        Set<List<Set<Index>>> cartesianProductOfPowerSetsPerReferencedTable;

        for (Workload wl : workloads(env.getWorkloadFolders())) {
            System.out.println("--------------------------");
            System.out.println("Processing workload: " + wl.getName());
            //Workload wl = workload(env.getWorkloadsFoldername() + "/tpch");
            final Set<Index> allIndexes = candGen.generate(wl);

            System.out.println("Candidates generated: " + allIndexes.size());

            System.out.println(
                    "query number, " +
                    "candidate subsets for query, " +
                    "candidate set size, " +
                    "INUM prepare time, " +
                    "INUM cost, " +
                    "optimizer cost, " +
                    "INUM explain time, " +
                    "optimizer explain time, " +
                    "indexes used by INUM, " +
                    "indexes used by optimizer, " +
                    "difference in index usage, " +
                    "optimizer using index intersection or union, " +
                    "optimizer using NLJ, " +
                    "optimizer / INUM");

            //System.out.println(
            //"Candidate subsets generated for : " + 
            //cartesianProductOfPowerSetsPerReferencedTable.size());

            for (SQLStatement sql : wl) {

                cartesianProductOfPowerSetsPerReferencedTable =
                    powerSetPerTable(allIndexes, delegate.explain(sql).getPlan().getTables());

                time = System.nanoTime();

                final PreparedSQLStatement pSql = optimizer.prepareExplain(sql);

                prepareTime = System.nanoTime() - time;

                j = 1;

                // less variation but deterministic (good for repeating results) {
                //for (Set<Index> conf : powerSet(allIndexes)) {
                // }

                // more variation but with a randomness element {
                for (List<Set<Index>> indexSetPerTable :
                        cartesianProductOfPowerSetsPerReferencedTable) {
                    Set<Index> conf = new HashSet<Index>();
                    for (Set<Index> idxs : indexSetPerTable)
                        conf.addAll(idxs);
                //}

                    time = System.nanoTime();

                    prepared = pSql.explain(conf);

                    explainTimePrepared = System.nanoTime() - time;

                    explainWhatIfCount =
                        delegate.getWhatIfCount() - totalWhatIfCount - prepareWhatIfCount;

                    totalWhatIfCount += prepareWhatIfCount + explainWhatIfCount;

                    time = System.nanoTime();

                    explained = delegate.explain(sql, conf);

                    explainTimeExplained = System.nanoTime() - time;

                    totalWhatIfCount--;

                    System.out.println(
                            i + "," +
                            prepareTime + "," +
                            cartesianProductOfPowerSetsPerReferencedTable.size() + "," +
                            conf.size() + "," +
                            explained.getSelectCost() + "," +
                            prepared.getSelectCost() + "," +
                            explainTimePrepared + "," +
                            explainTimeExplained + "," +
                            prepared.getUsedConfiguration().size() + "," +
                            explained.getUsedConfiguration().size() + "," +
                            getNumberOfDistinctIndexesByContent(
                                prepared.getUsedConfiguration(),
                                explained.getUsedConfiguration()) + "," +
                            ((explained.getPlan().contains(INDEX_AND) || 
                              explained.getPlan().contains(INDEX_OR)) ? "Y" : "N") + "," +
                            (explained.getPlan().contains(NESTED_LOOP_JOIN) ? "Y" : "N") + "," +
                            prepared.getSelectCost() / explained.getSelectCost());

                    if (j++ == stopCount)
                        break;
                }

                i++;

            }
        }
        afterClass();
    }

    /**
     * stuff.
     *
     * @param indexes
     *      indexes
     * @param tables
     *      tables referenced in stmt
     * @return
     *      set of sets of indexes
     */
    private static Set<List<Set<Index>>> powerSetPerTable(Set<Index> indexes, List<Table> tables)
    {
        Set<Index> indexesForTable;
        Set<Index> smaller;
        Set<Set<Index>> powerSetForTable;
        List<Set<Set<Index>>> powerSets;
        int powerSetLimit;

        powerSets = new ArrayList<Set<Set<Index>>>();

        if (tables.size() == 1)
            powerSetLimit = 16;
        else if (tables.size() == 2)
            powerSetLimit = 8;
        else
            powerSetLimit = 4;

        for (Table table : tables) {

            indexesForTable = getIndexesReferencingTable(indexes, table);

            if (indexesForTable.size() > powerSetLimit) {
                smaller = new HashSet<Index>();

                for (int i = 0; i < powerSetLimit; i++)
                    while(!smaller.add(
                                get(indexesForTable, RANDOM.nextInt(indexesForTable.size()))));

                indexesForTable = smaller;
            }

            powerSetForTable = powerSet(indexesForTable);

            powerSets.add(powerSetForTable);
        }

        return cartesianProduct(powerSets);
    }
}
