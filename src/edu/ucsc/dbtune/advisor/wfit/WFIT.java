package edu.ucsc.dbtune.advisor.wfit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.sql.SQLException;

import edu.ucsc.dbtune.advisor.Advisor;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * XXX #110 class Selector from Karl's repository should replace WFIT, since that's were the truly 
 * ONLINE mode is implemented.
 */
public class WFIT extends Advisor
{
    List<PreparedSQLStatement> qinfos;
    List<Double> overheads;
    List<Set<Index>> configurations;

    Set<Index> indexes;
    WorkFunctionAlgorithm wfa;
    Optimizer optimizer;

    int maxNumIndexes;
    int maxNumStates;
    int windowSize;
    int partitionIterations;

    /**
     */
    public WFIT(Optimizer optimizer, Set<Index> configuration,
            int maxNumIndexes, int maxNumStates, int windowSize,
            int partitionIterations) {
        this.indexes = configuration;
        this.maxNumIndexes = maxNumIndexes;
        this.maxNumStates = maxNumStates;
        this.windowSize = windowSize;
        this.partitionIterations = partitionIterations;
        this.optimizer = optimizer;
        this.qinfos = new ArrayList<PreparedSQLStatement>();
        this.wfa = new WorkFunctionAlgorithm(configuration, maxNumStates,
                maxNumIndexes);
        this.overheads = new ArrayList<Double>();
        this.configurations = new ArrayList<Set<Index>>();
    }

    /**
     * Adds a query to the set of queries that are considered for
     * recommendation.
     * 
     * @param sql
     *            sql statement
     * @throws SQLException
     *             if the given statement can't be processed
     */
    @Override
    public void process(SQLStatement sql) throws SQLException
    {
        PreparedSQLStatement qinfo;

        qinfo = optimizer.prepareExplain(sql);

        qinfos.add(qinfo);

        // partitions =
        // getIndexPartitions(
        // indexes, qinfos, maxNumIndexes, maxNumStates, windowSize,
        // partitionIterations);

        // wfa.repartition(partitions);
        wfa.newTask(qinfo,indexes);

        configurations.add(new HashSet<Index>(wfa.getRecommendation()));
    }

    /**
     * Returns the configuration obtained by the Advisor.
     * 
     * @return a {@code Set<Index>} object containing the information related
     *         to the recommendation produced by the advisor.
     * @throws SQLException
     *             if the given statement can't be processed
     */
    @Override
    public Set<Index> getRecommendation() throws SQLException
    {
        if (qinfos.size() == 0) {
            return new HashSet<Index>();
        }

        return configurations.get(qinfos.size() - 1);
    }

    public PreparedSQLStatement getStatement(int i)
    {
        return qinfos.get(i);
    }
    /*
     * public IndexPartitions getPartitions()
 { return partitions; }
     * 
     * private IndexPartitions getIndexPartitions( Set<Index> candidateSet,
     * List<ExplainedSQLStatement> qinfos, int maxNumIndexes, int maxNumStates,
     * int windowSize, int partitionIterations ) { IndexStatisticsFunction
     * benefitFunc = new IndexStatisticsFunction(windowSize);
     * 
     * Set<Index> hotSet = chooseGreedy( candidateSet, new
     * Set<Index>("old"), new Set<Index>("required"), benefitFunc,
     * maxNumIndexes, false);
     * 
     * return choosePartitions( candidateSet, hotSet, partitions, benefitFunc,
     * maxNumStates, partitionIterations); }
     */
}
