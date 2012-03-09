package edu.ucsc.dbtune.optimizer;

import java.util.Comparator;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;

import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
//import static edu.ucsc.dbtune.util.TestUtils.workloads;
import static edu.ucsc.dbtune.util.TestUtils.workload;

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
        delegate = getBaseOptimizer(optimizer);
        candGen = CandidateGenerator.Factory.newCandidateGenerator(env, delegate);
        
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
        if (delegate == null) return;

        i = 0;
        //for (Workload wl : workloads(env.getWorkloadFolders())) {
        Workload wl = workload(env.getWorkloadsFoldername() + "/tpch-small");
        final Set<Index> allIndexes = candGen.generate(wl);

        System.out.println("wlname, stmt, optimizer cost, delegate cost");

        for (SQLStatement sql : wl) {
            i++;

            final PreparedSQLStatement pSql = optimizer.prepareExplain(sql);

            ExplainedSQLStatement explainedByOptimizer = pSql.explain(allIndexes);
            ExplainedSQLStatement explainedByDelegate = delegate.explain(sql, allIndexes);

            assertThat(
                    "Optimizer: " + explainedByOptimizer + "\n" +
                    "Delegate: " + explainedByDelegate,
                    compare(explainedByOptimizer, explainedByDelegate), is(0));

            System.out.println(
                    wl.getName() + "," +
                    i + "," +
                    explainedByOptimizer.getSelectCost() + "," +
                    explainedByDelegate.getSelectCost() + "\n");
        }

        //}
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compare(ExplainedSQLStatement e1, ExplainedSQLStatement e2)
    {
        if (e1.statement.equals(e2.statement) &&
                (e1.selectCost / e2.selectCost) > 0.90 &&
                (e1.selectCost / e2.selectCost) < 1.10 &&
                e1.updateCost == e2.updateCost &&
                e1.configuration.equals(e2.configuration))
            return 0;

        System.out.println(
                "Statement " + i + "\n" +
                "Optimizer:\n" + e1.getPlan() +
                "\n\nvs\n\nDelegate:\n" + e2.getPlan());

        if ((e1.selectCost / e2.selectCost) < 0.90)
            return -1;

        return 1;
    }
}
