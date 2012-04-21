package edu.ucsc.dbtune.advisor.wfit;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.advisor.Advisor;
import edu.ucsc.dbtune.advisor.interactions.DegreeOfInteractionFinder;
import edu.ucsc.dbtune.advisor.interactions.IBGDoiFinder;
import edu.ucsc.dbtune.advisor.interactions.InteractionBank;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.optimizer.IBGPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 *
 * @author Ivo Jimenez
 */
public class WFIT extends Advisor
{
    private IBGOptimizer ibgOptimizer;
    private Selector selector;
    private Set<Index> pool;
    private DegreeOfInteractionFinder doiFinder;

    /**
     * Creates a WFIT advisor.
     *
     * @param optimizer
     *      used to execute what-if calls
     * @param pool
     *      candidate pool
     */
    public WFIT(IBGOptimizer optimizer, Set<Index> pool)
    {
        this.ibgOptimizer = optimizer;
        this.pool = pool;

        selector = new Selector();
        doiFinder = new IBGDoiFinder();
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
        int whatIfCountBefore = ibgOptimizer.getWhatIfCount();
        IBGPreparedSQLStatement pStmt = (IBGPreparedSQLStatement) ibgOptimizer.prepareExplain(sql);
        ExplainedSQLStatement eStmt = pStmt.explain(pool);
        int whatIfCountAfter = ibgOptimizer.getWhatIfCount();
        InteractionBank bank = doiFinder.degreeOfInteraction(pStmt, pool);

        selector.analyzeQuery(
            new ProfiledQuery(
                sql.getSQL(),
                eStmt,
                getSnapshot(pool),
                pStmt.getIndexBenefitGraph(),
                bank,
                whatIfCountAfter - whatIfCountBefore));
    }

    /**
     * Creates a {@link CandidatePool.Snapshot} out of a index set.
     *
     * @param indexes
     *      set from which the snapshot is created
     * @return
     *      the snapshot
     * @throws SQLException
     *      if an error occurs while adding indexes to the snapshot
     */
    private CandidatePool.Snapshot getSnapshot(Set<Index> indexes)
        throws SQLException
    {
        CandidatePool pool = new CandidatePool();

        for (Index i : indexes)
            pool.addIndex(i);

        return pool.getSnapshot();
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
        Set<Index> recommendation = new HashSet<Index>();

        for (Index idx : selector.getRecommendation())
            recommendation.add(idx);

        return recommendation;
    }

    /**
     * @param i
     *      i
     * @return
     *      return
     */
    public PreparedSQLStatement getStatement(int i)
    {
        throw new RuntimeException("Not yet");
    }
}
