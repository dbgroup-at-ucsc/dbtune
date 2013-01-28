package edu.ucsc.dbtune.optimizer;

import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static java.lang.Math.min;

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
import static edu.ucsc.dbtune.util.MetadataUtils.getNumberOfDistinctIndexes;
import static edu.ucsc.dbtune.util.OptimizerUtils.getBaseOptimizer;
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
    private static ExplainedSQLStatement explainedByInum;
    private static ExplainedSQLStatement explainedByOptimizer;
    private static DatabaseSystem db;
    private static Environment env;
    private static Optimizer optimizer;
    private static Optimizer delegate;
    private static SQLStatement currentSql;
    private static CandidateGenerator candGen;
    private static List<SQLStatement> wl;
    private static int numberOfSubsets;
    private static long time;
    private static long prepareTime;
    private static long inumTime;
    private static long optimizerTime;
    private static StringBuilder crashReport;

    //############################################################################
    // CONTROL PARAMETERS
    //############################################################################

    /**
     * Option for the subset enumeration.
     */
    private static String subsetOption = POWERSET;

    /**
     * Name of the workload to run the experiment on.
     */
    private static String workloadName = "tpcds-small";

    /**
     * Number of maximum what-if calls on a statement. If {@link #numberOfSubsets} is less than this 
     * number, then {@link #numberOfSubsets} is the stop count.
     */
    private static int maxNumberOfWhatIfCallsPerStatement = 500;

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

        if (!(optimizer instanceof InumOptimizer))
            throw new Exception("Expecting INUM optimizer");
        
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
        System.out.println("Processing workload: " + wl.get(0).getWorkload().getWorkloadName());

        allIndexes = candGen.generate(wl);

        System.out.println("Candidates generated: " + allIndexes.size());

        //CHECKSTYLE:OFF
        System.out.println(
                "workload name, " +                                                 // A
                "query number, " +                                                  // B
                "candidate subsets for query, " +                                   // C
                "candidate set size, " +                                            // D
                "INUM prepare time, " +                                             // E
                "INUM cost, " +                                                     // F
                "optimizer cost, " +                                                // G
                "cost ratio (optimizer cost / INUM cost), " +                       // H
                "optimizer cost / (optimizer cost / STDEV(cost ratio)), " +         // I
                "INUM explain time, " +                                             // J
                "optimizer explain time, " +                                        // K
                "explain time ratio (optimizer explain time/ INUM explain time)," + // L
                "optimizer explain time / AVG INUM per-whatif explain time," +      // M
                "indexes used by INUM, " +
                "indexes used by optimizer, " +
                "difference in index usage, " +
                "optimizer using index intersection or union, " +
                "optimizer using NLJ, " +
                "INUM using NLJ");

        int queryNumber = 0;
        int whatIfCallNumber = 0;
        int totalNumberOfWhatIfCalls = 0;
        int entriesForStmt = 0;
        int indexForEntry = 0;
        int startedAt = 0;
        int stoppedAt = 0;

        for (SQLStatement sql : wl) {
            currentSql = sql;
            queryNumber++;

            try {
                time = System.nanoTime();

                final PreparedSQLStatement pSql = optimizer.prepareExplain(sql);

                prepareTime = System.nanoTime() - time;

                totalNumberOfWhatIfCalls += whatIfCallNumber;
                whatIfCallNumber = 0;

                for (Set<Index> conf : subsets()) {

                    whatIfCallNumber++;

                    time = System.nanoTime();
                    explainedByInum = pSql.explain(conf);
                    inumTime = System.nanoTime() - time;
                    time = System.nanoTime();
                    explainedByOptimizer = delegate.explain(sql, conf);
                    optimizerTime = System.nanoTime() - time;

                    entriesForStmt = min(numberOfSubsets, maxNumberOfWhatIfCallsPerStatement);
                    startedAt = totalNumberOfWhatIfCalls + 1;
                    stoppedAt = totalNumberOfWhatIfCalls + entriesForStmt;
                    indexForEntry = startedAt + whatIfCallNumber - 1;

                    System.out.println(
                            sql.getWorkload().getWorkloadName() + "," +
                            queryNumber + "," +
                            numberOfSubsets + "," +
                            conf.size() + "," +
                            prepareTime + "," +
                            explainedByInum.getSelectCost() + "," +
                            explainedByOptimizer.getSelectCost() + "," +
                            "=G" + indexForEntry + "/F" + indexForEntry + "," +
                            "=G" + indexForEntry + "/G" + (stoppedAt + 1) + "," + // < G(last+1) is G from below
                            inumTime + "," +
                            optimizerTime + "," +
                            "=K" + indexForEntry + "/J" + indexForEntry + "," +
                            "=K" + indexForEntry + "/L" + (stoppedAt + 1) + "," + // < L(last+1) is L from below
                            explainedByInum.getUsedConfiguration().size() + "," +
                            explainedByOptimizer.getUsedConfiguration().size() + "," +
                            getNumberOfDistinctIndexes(
                                explainedByInum.getUsedConfiguration(),
                                explainedByOptimizer.getUsedConfiguration()) + "," +
                            ((explainedByOptimizer.getPlan().contains(INDEX_AND) || 
                              explainedByOptimizer.getPlan().contains(INDEX_OR)) ? "Y" : "N") + "," +
                            (explainedByOptimizer.getPlan().contains(NESTED_LOOP_JOIN) ? "Y" : "N") + "," +
                            (explainedByInum.getPlan().contains(NESTED_LOOP_JOIN) ? "Y" : "N"));

                    if (whatIfCallNumber == maxNumberOfWhatIfCallsPerStatement)
                        break;
                }
                /*
                  A - TOTAL
                  B - Statement
                  C - INUM prepare time (once)

                  Cost :

                  D - INUM cost (AVERAGE(inum-cost_i))
                  E - optimizer cost (AVERAGE(optimizer-cost_i))
                  F - cost ratio (AVERAGE(optimizer-cost_i / inum-cost_i))
                  G - INUM cost deviation (STDEV(inum-cost_i))
                  H - deviation ratio (AVERAGE(optimizer-cost_i) / AVERAGE(optimizer-cost_i / STDEV(inum-cost)))

                  Time:

                  I - INUM explain time (AVERAGE(INUM-explain-time_i))
                  J - optimizer explain time (AVERAGE(optimizer-explain-time_i))
                  K - explain time ratio (AVERAGE(optimizer-explain-time_i / INUM-explain-time_i))
                  L - INUM avg what-if (INUM prepare / #what-if calls + avg explain)
                  M - Overall ratio (AVERAGE(optimizer explain time / (INUM prepare / #what-if calls + avg explain)))
                */

                if (startedAt == 0)
                    startedAt = 1;

                System.out.println(
                        "TOTAL," +                                                               // A
                        queryNumber + "," +                                                      // B
                        prepareTime + "," +                                                      // C
                        "=AVERAGE(F" + startedAt + ":F" + stoppedAt + ")," +                     // D
                        "=AVERAGE(G" + startedAt + ":G" + stoppedAt + ")," +                     // E
                        "=AVERAGE(H" + startedAt + ":H" + stoppedAt + ")," +                     // F
                        "=IF(STDEV(H" + startedAt + ":H" + stoppedAt + ")<1; 1; STDEV(H" + startedAt + ":H" + stoppedAt + "))," + // G
                        "=G" + stoppedAt + "/AVERAGE(I" + startedAt + ":I" + stoppedAt + ")," + // H
                        "=AVERAGE(J" + startedAt + ":J" + stoppedAt + ")," +                     // I
                        "=AVERAGE(K" + startedAt + ":K" + stoppedAt + ")," +                     // J
                        "=AVERAGE(L" + startedAt + ":L" + stoppedAt + ")," +                     // K
                        "=" + prepareTime / entriesForStmt + " + I" + (stoppedAt + 1) + "," +    // L
                        "=AVERAGE(M" + startedAt + ":M" + stoppedAt + ")");                      // M

                // we need to add one for the aggregate
                totalNumberOfWhatIfCalls++;
            } catch (Exception ex) {
            //CHECKSTYLE:ON
                StringWriter sw = new StringWriter();
                ex.printStackTrace(new PrintWriter(sw));
                crashReport
                    .append("------------------------------")
                    .append("Workload " + sql.getWorkload().getWorkloadName() + "\n")
                    .append("Statement " + queryNumber + "\n")
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
        else
            powerSetLimit = 4;

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
