package edu.ucsc.dbtune.optimizer;

import java.util.Comparator;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.OptimizerUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
import static edu.ucsc.dbtune.util.TestUtils.workloads;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

import static org.junit.Assert.assertThat;

/**
 * Functional test for testing non-dbms optimizers against the corresponding dbms one. The optimizer 
 * being tested is specified by the {@link edu.ucsc.dbtune.util.EnvironmentProperties#OPTIMIZER} 
 * property. The base optimizer, i.e. the implementation of {@link Optimizer} that runs right on top 
 * of the DBMS (e.g. {@link DB2Optimizer}) is retrieved through the {@link #getBaseOptimizer} 
 * utility method.
 *
 * @author Ivo Jimenez
 */
public class OptimizerVsDelegateFunctionalTest implements Comparator<ExplainedSQLStatement>
{
    private static DatabaseSystem db;
    private static Environment env;
    private static Optimizer optimizer;
    private static Optimizer delegate;
    private static CandidateGenerator candGen;
    private static PreparedSQLStatement pSql;
    private int i;

    /**
     * @throws Exception
     *      if {@link #newDatabaseSystem} throws an exception
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        env = Environment.getInstance();
        db = newDatabaseSystem(env);
        optimizer = db.getOptimizer();
        delegate = optimizer.getDelegate();
        candGen =
            CandidateGenerator.Factory.newCandidateGenerator(env, getBaseOptimizer(optimizer));
        
        loadWorkloads(db.getConnection());
    }

    /**
     * @throws Exception
     *      if something goes wrong while closing the connection to the dbms
     */
    @AfterClass
    public static void afterClass() throws Exception
    {
        db.getConnection().close();
    }

    /**
     * @throws Exception
     *      if something goes wrong
     * @see OptimizerTest#checkPreparedExplain
     */
    @Test
    public void testPreparedSQLStatement() throws Exception
    {
        if (delegate == null || optimizer == delegate) return;

        System.out.println("wlname, stmt, optimizer cost, delegate cost");

        for (Workload wl : workloads(env.getWorkloadFolders())) {

            final Set<Index> allIndexes = candGen.generate(wl);

            i = 0;

            for (SQLStatement sql : wl) {
                i++;

                if (getBaseOptimizer(optimizer) instanceof MySQLOptimizer && 
                        sql.getSQLCategory().isSame(SQLCategory.NOT_SELECT))
                    // issue #106
                    continue;

                System.out.print(wl.getName() + "," + i + ",");

                pSql = optimizer.prepareExplain(sql);

                ExplainedSQLStatement explainedByOptimizer = pSql.explain(allIndexes);
                ExplainedSQLStatement explainedByDelegate = delegate.explain(sql, allIndexes);

                if (Class.forName("edu.ucsc.dbtune.optimizer.InumOptimizer").isInstance(optimizer))
                    // for INUM, over-estimations are allowed; under-estimations will cause failure
                    assertThat(
                            getErrorString(explainedByOptimizer, explainedByDelegate),
                            compare(explainedByOptimizer, explainedByDelegate),
                            is(greaterThanOrEqualTo(0)));
                else if (optimizer instanceof IBGOptimizer)
                    // IBG should be exactly the same
                    assertThat(
                            getErrorString(explainedByOptimizer, explainedByDelegate),
                            explainedByOptimizer.equalsIgnorePlan(explainedByDelegate), is(true));
                else
                    throw new RuntimeException(
                            "Unknown optimizer class: " + optimizer.getClass().getName());

                System.out.print(
                        explainedByOptimizer.getSelectCost() + "," +
                        explainedByDelegate.getSelectCost() + "\n");
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compare(ExplainedSQLStatement e1, ExplainedSQLStatement e2)
    {
        double ratio =
            (e1.selectCost + e1.baseTableUpdateCost) /
            (e2.selectCost + e2.baseTableUpdateCost);

        if (e1.statement.equals(e2.statement) && ratio > 0.90 && ratio < 1.10)
            return 0;

        if (ratio > 1.10)
            return 1;

        return -1;
    }

    /**
     * Returns an error string.
     *
     * @param e1
     *      explained statement
     * @param e2
     *      explained statement
     * @return
     *      error string
     */
    public String getErrorString(ExplainedSQLStatement e1, ExplainedSQLStatement e2)
    {
        return "### Error\n" +
            "Statement " + i + "\n" +
            "Optimizer Statement text: " + e1.getStatement() + "\n" +
            "Delegate Statement text:  " + e2.getStatement() + "\n" +
            "Optimizer total cost: " + e1.getTotalCost() + "\n" +
            "Delegate total cost:  " + e2.getTotalCost() + "\n" +
            "Optimizer base table update cost: " + e1.getBaseTableUpdateCost() + "\n" +
            "Delegate base table update cost:  " + e2.getBaseTableUpdateCost() + "\n" +
            "Optimizer select cost: " + e1.getSelectCost() + "\n" +
            "Delegate select cost:  " + e2.getSelectCost() + "\n" +
            "Optimizer index update costs: " + e1.getIndexUpdateCosts() + "\n" +
            "Delegate index update costs:  " + e2.getIndexUpdateCosts() + "\n" +
            "Optimizer updated table: " + e1.getUpdatedTable() + "\n" +
            "Delegate updated table:  " + e2.getUpdatedTable() + "\n" +
            "Optimizer conf: " + e1.getConfiguration() + "\n" +
            "Delegate conf:  " + e2.getConfiguration() + "\n" +
            "Optimizer used conf: " + e1.getUsedConfiguration() + "\n" +
            "Delegate used conf:  " + e2.getUsedConfiguration() + "\n" +
            "Optimizer updated conf: " + e1.getUpdatedConfiguration() + "\n" +
            "Delegate updated conf:  " + e2.getUpdatedConfiguration() + "\n" +
            "Optimizer:\n" + e1.getPlan() +
            "\n\nvs\n\nDelegate:\n" + e2.getPlan();
    }
}
