package edu.ucsc.dbtune.advisor.wfit;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.advisor.Advisor;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

/**
 * XXX #110 class Selector from Karl's repository should replace WFIT, since that's were the truly 
 * ONLINE mode is implemented.
 *
 * @author Ivo Jimenez
 */
public class WFIT extends Advisor
{
    //private Optimizer optimizer;

    /**
     * Creates a WFIT advisor.
     *
     * @param optimizer
     *      used to execute what-if calls
     */
    public WFIT(Optimizer optimizer)
    {
        //this.optimizer = optimizer;
    }

    /**
     * Adds the statements contained in a workload to the set of statements that are considered for 
     * recommendation.
     *
     * @param workload
     *      sql statements
     * @throws SQLException
     *      if the given statements can't be processed
     */
    @Override
    public void process(Workload workload) throws SQLException
    {
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
        throw new RuntimeException("Not yet");
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
