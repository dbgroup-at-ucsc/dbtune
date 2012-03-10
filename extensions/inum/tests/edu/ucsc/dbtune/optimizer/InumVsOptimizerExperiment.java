package edu.ucsc.dbtune.optimizer;

import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Sets;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static com.google.common.collect.Iterables.get;
import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.optimizer.plan.Operator.INDEX_AND;
import static edu.ucsc.dbtune.optimizer.plan.Operator.INDEX_OR;
import static edu.ucsc.dbtune.optimizer.plan.Operator.NESTED_LOOP_JOIN;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesReferencingTable;
import static edu.ucsc.dbtune.util.MetadataUtils.getNumberOfDistinctIndexesByContent;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
import static edu.ucsc.dbtune.util.TestUtils.workload;

/**
 * @author Ivo Jimenez
 */
public final class InumVsOptimizerExperiment
{
    private static final String ONE                = "one";
    private static final String POWERSET           = "powerSet";
    private static final String POWERSET_PER_TABLE = "powerSetPerTable";
    private static final Random R                  = new Random(System.currentTimeMillis());

    private static Set<Index> allIndexes;
    private static ExplainedSQLStatement prepared;
    private static ExplainedSQLStatement explained;
    private static DatabaseSystem db;
    private static Environment env;
    private static Optimizer optimizer;
    private static Optimizer delegate;
    private static SQLStatement currentSql;
    private static CandidateGenerator candGen;
    private static Workload wl;
    private static int prepareWhatIfCount;
    private static int explainWhatIfCount;
    private static int totalWhatIfCount;
    private static int numberOfSubsets;
    private static long time;
    private static long prepareTime;
    private static long explainTimePrepared;
    private static long explainTimeExplained;
    private static StringBuilder crashReport;

    //############################################################################
    // CONTROL PARAMETERS
    //############################################################################

    /**
     * Option for the subset enumeration.
     */
    private static String subsetOption = ONE;

    /**
     * Name of the workload to run the experiment on.
     */
    private static String workloadName = "tpch";

    /**
     * Number of maximum what-if calls on a statement. If {@link #numberOfSubsets} is less than this 
     * number, then {@link #numberOfSubsets} is the stop count.
     */
    private static int stopCount = 500;

    //############################################################################

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
        crashReport = new StringBuilder();
        
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
     * The subsets on which to execute what-if calls, based on the value that {@link subsetOption} 
     * has.
     *
     * @return
     *      subsets
     * @throws Exception
     *      if error
     */
    public static Iterable<Set<Index>> subsets() throws Exception
    {
        if (subsetOption.equals(ONE))
            return oneSubset();

        if (subsetOption.equals(POWERSET))
            return powerSet();

        if (subsetOption.equals(POWERSET_PER_TABLE))
            return powerSetPerTable();

        throw new RuntimeException("Unkown option " + subsetOption);
    }

    /**
     * run experiment.
     *
     * @param args
     *      arguments
     * @throws Exception
     *      if something goes wrong
     */
    public static void main(String[] args) throws Exception
    {
        beforeClass();

        if (delegate == null)
            throw new Exception("No delegate (base optimizer), check configuration");

        wl = workload(env.getWorkloadsFoldername() + "/" + workloadName);

        System.out.println("--------------------------");
        System.out.println("Processing workload: " + wl.getName());

        allIndexes = candGen.generate(wl);

        System.out.println("Candidates generated: " + allIndexes.size());

        System.out.println(
                "workload name, " +
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

        int i = 0;
        int j;

        for (SQLStatement sql : wl) {
            currentSql = sql;
            i++;

            try {
                time = System.nanoTime();

                final PreparedSQLStatement pSql = optimizer.prepareExplain(sql);

                prepareTime = System.nanoTime() - time;

                j = 0;

                for (Set<Index> conf : subsets()) {

                    j++;

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
                            wl.getName() + "," +
                            i + "," +
                            prepareTime + "," +
                            numberOfSubsets + "," +
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

                    if (j == stopCount)
                        break;
                }
            //CHECKSTYLE:OFF
            } catch (Exception ex) {
            //CHECKSTYLE:ON
                StringWriter sw = new StringWriter();
                ex.printStackTrace(new PrintWriter(sw));
                crashReport
                    .append("------------------------------")
                    .append("Workload " + wl.getName() + "\n")
                    .append("Statement " + i + "\n")
                    .append("SQL " + sql.getSQL() + "\n")
                    .append("Error msg: " + ex.getMessage() + "\n")
                    .append("Stack:\n" + sw.toString() + "\n");
                continue;
            }
        }
        System.out.println(crashReport.toString());
        afterClass();
    }

    /**
     * Returns one set containing {@link #allIndexes}.
     *
     * @return
     *      an iterable over the subsets
     */
    private static Iterable<Set<Index>> oneSubset()
    {
        Set<Set<Index>> onlyOneSubSet = new HashSet<Set<Index>>();
        onlyOneSubSet.add(allIndexes);
        numberOfSubsets = onlyOneSubSet.size();
        return onlyOneSubSet;
    }

    /**
     * Returns the powerset of {@link #allIndexes}.
     *
     * @return
     *      an iterable over the subsets
     * @throws Exception
     *      if an error occurs while creating the powerset
     */
    private static Iterable<Set<Index>> powerSet() throws Exception
    {
        Set<Index> indexes = new HashSet<Index>();

        if (allIndexes.size() > 31)
            for (int i = 0; i < 32; i++)
                indexes.add(get(allIndexes, R.nextInt(allIndexes.size())));
        else
            indexes = allIndexes;

        Set<Set<Index>> subsets = Sets.powerSet(indexes);

        numberOfSubsets = subsets.size();

        return subsets;
    }

    /**
     * Creates a list of power sets per table referenced by {@link #currentSql}.
     *
     * @return
     *      an iterable over the subsets
     * @throws Exception
     *      if an error occurs while creating the power set per table
     */
    private static Iterable<Set<Index>> powerSetPerTable() throws Exception
    {
        Set<List<Set<Index>>> cartesianProductOfPowerSetsPerReferencedTable;

        cartesianProductOfPowerSetsPerReferencedTable =
            powerSetPerTable(allIndexes, delegate.explain(currentSql).getPlan().getTables());

        numberOfSubsets = cartesianProductOfPowerSetsPerReferencedTable.size();

        return new PowerSetPerTableIterable(cartesianProductOfPowerSetsPerReferencedTable);
    }

    /**
     * Returns a set of lists, each containing the power set of indexes (obtained from {@code 
     * indexes}) corresponding to a table from {@code tables}.
     *
     * @param indexes
     *      indexes
     * @param tables
     *      tables referenced in a statement
     * @return
     *      set of lists, where each contains the powerset of indexes referencing a table
     */
    private static Set<List<Set<Index>>> powerSetPerTable(Set<Index> indexes, List<Table> tables)
    {
        Set<Index> indexesForTable;
        Set<Index> smaller;
        Set<Set<Index>> powerSetForTable;
        List<Set<Set<Index>>> powerSets;
        int powerSetLimit;
        boolean added;

        powerSets = new ArrayList<Set<Set<Index>>>();

        if (tables.size() == 1)
            powerSetLimit = 16;
        else if (tables.size() == 2)
            powerSetLimit = 8;
        else if (tables.size() == 3)
            powerSetLimit = 4;
        else
            powerSetLimit = 3;

        for (Table table : tables) {

            indexesForTable = getIndexesReferencingTable(indexes, table);

            if (indexesForTable.size() > powerSetLimit) {
                smaller = new HashSet<Index>();

                for (int i = 0; i < powerSetLimit; i++)
                    do {
                        added =
                            smaller.add(get(indexesForTable, R.nextInt(indexesForTable.size())));
                    } while (!added);

                indexesForTable = smaller;
            }

            powerSetForTable = Sets.powerSet(indexesForTable);

            powerSets.add(powerSetForTable);
        }

        return Sets.cartesianProduct(powerSets);
    }

    /**
     */
    private static class PowerSetPerTableIterable
        implements Iterator<Set<Index>>, Iterable<Set<Index>>
    {
        private Iterator<List<Set<Index>>> delegate;
        
        /**
         * constructor.
         *
         * @param productOfPowerSetsPerReferencedTable
         *      the cartesian product
         */
        public PowerSetPerTableIterable(Set<List<Set<Index>>> productOfPowerSetsPerReferencedTable)
        {
            delegate = productOfPowerSetsPerReferencedTable.iterator();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Set<Index> next()
        {
            List<Set<Index>> indexSetPerTable = delegate.next();
            Set<Index> conf = new HashSet<Index>();
            for (Set<Index> idxs : indexSetPerTable)
                conf.addAll(idxs);
            return conf;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("Can't remove");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Iterator<Set<Index>> iterator()
        {
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext()
        {
            return delegate.hasNext();
        }
    }
}
