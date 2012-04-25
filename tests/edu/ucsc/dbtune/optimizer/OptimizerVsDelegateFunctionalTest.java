package edu.ucsc.dbtune.optimizer;

import java.util.Comparator;
import java.util.Random;
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

import static com.google.common.collect.Iterables.get;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
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
    private static Random r;
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
        r = new Random(System.currentTimeMillis());
        
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
        if (optimizer == delegate) return;

        System.out.println("wlname, stmt, optimizer cost, delegate cost");

        for (Workload wl : workloads(env.getWorkloadFolders())) {

            final Set<Index> allIndexes = candGen.generate(wl);

            i = 0;

            for (SQLStatement sql : wl) {
                i++;

                if (optimizer instanceof IBGOptimizer &&
                        (sql.getSQLCategory().isSame(SQLCategory.NOT_SELECT) ||
                         r.nextInt(100) > 5))
                    continue;

                System.out.print(wl.getName() + "," + i + ",");

                pSql = optimizer.prepareExplain(sql);

                ExplainedSQLStatement explainedByOptimizer = pSql.explain(allIndexes);
                ExplainedSQLStatement explainedByDelegate = delegate.explain(sql, allIndexes);

                // over-estimations by a delegate are allowed, while under-estimations are 
                // considered a failure
                assertThat(
                        compare(explainedByOptimizer, explainedByDelegate),
                        is(greaterThanOrEqualTo(0)));

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

        System.out.println(
                "### Error\n" +
                "Statement " + i + "\n" +
                "Optimizer total cost: " + e1.getTotalCost() + "\n" +
                "Delegate total cost: " + e2.getTotalCost() + "\n" +
                "Optimizer base table update cost: " + e1.getBaseTableUpdateCost() + "\n" +
                "Delegate base table update cost: " + e2.getBaseTableUpdateCost() + "\n" +
                "Optimizer select cost: " + e1.getSelectCost() + "\n" +
                "Delegate select cost: " + e2.getSelectCost() + "\n" +
                "Optimizer base table update cost: " + e1.getBaseTableUpdateCost() + "\n" +
                "Delegate base table update cost: " + e2.getBaseTableUpdateCost() + "\n" +
                "Optimizer:\n" + e1.getPlan() +
                "\n\nvs\n\nDelegate:\n" + e2.getPlan());

        return -1;
    }
}
