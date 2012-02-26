package edu.ucsc.dbtune.optimizer;

import java.sql.ResultSet;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;

import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
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

//import static org.hamcrest.Matchers.is;
//import static org.junit.Assert.assertThat;

/**
 * Functional test for testing non-dbms optimizers against the corresponding dbms one. The optimizer 
 * being tested is specified by the {@link edu.ucsc.dbtune.util.EnvironmentProperties#OPTIMIZER} 
 * property. The base optimizer, i.e. the implementation of {@link Optimizer} that runs right on top 
 * of the DBMS (e.g. {@link DB2Optimizer}) is retrieved through the {@link #getBaseOptimizer} 
 * utility method.
 *
 * @author Ivo Jimenez
 */
public class OptimizerVsDelegateFunctionalTest
{
    private static DatabaseSystem db;
    private static Environment env;
    private static Optimizer optimizer;
    private static Optimizer delegate;
    private static CandidateGenerator candGen;

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

        //Statement stmt = db.getConnection().createStatement();
        //ResultSet rs = stmt.executeQuery(
            //"SELECT PKGNAME, PKGSCHEMA " +
            //"FROM SYSCAT.PACKAGES WHERE QUERYOPT = CURRENT QUERY OPTIMIZATION");

        //while (rs.next())
            //System.out.println(rs.getString("pkgname") + " " + rs.getString("pkgname"));

        //stmt.execute("SET CURRENT QUERY OPTIMIZATION = 1");
        //for (Workload wl : workloads(env.getWorkloadFolders())) {
        Workload wl = workload(env.getWorkloadsFoldername() + "/tpch-small");
        final Set<Index> conf = candGen.generate(wl);

        System.out.println("Candidates generated: " + conf.size());

        for (Index i : conf)
            System.out.println("   " + i);

        int i = 1;
        int prepareWhatIfCount = 0;
        int explainWhatIfCount = 0;
        int totalWhatIfCount = 0;
        long time;
        long prepareTime;
        ExplainedSQLStatement prepared;
        ExplainedSQLStatement explained;
        List<SQLStatementPlan> optimizerPlans = new ArrayList<SQLStatementPlan>();
        List<SQLStatementPlan> delegatePlans = new ArrayList<SQLStatementPlan>();

        System.out.println(
            "query number, optimizer cost, delegate cost, prepare time, " +
            "prepare what-if count, delegate / optimizer");

        for (SQLStatement sql : wl) {
            time = System.currentTimeMillis();

            final PreparedSQLStatement pSql = optimizer.prepareExplain(sql);

            prepareWhatIfCount = delegate.getWhatIfCount() - totalWhatIfCount;

            prepareTime = System.currentTimeMillis() - time;
            time = System.currentTimeMillis();

            prepared = pSql.explain(conf);

            explainWhatIfCount = delegate.getWhatIfCount() - totalWhatIfCount - prepareWhatIfCount;
            totalWhatIfCount += prepareWhatIfCount + explainWhatIfCount;

            explained = delegate.explain(sql, conf);

            System.out.println(
                    i++ + "," +
                    explained.getSelectCost() + "," +
                    prepared.getSelectCost() + "," +
                    prepareTime + "," +
                    prepareWhatIfCount + "," +
                    prepared.getSelectCost() / explained.getSelectCost());

            optimizerPlans.add(explained.getPlan());
            delegatePlans.add(prepared.getPlan());

            //assertThat(pSql.explain(conf), is(delegate.explain(sql, conf)));
        }

        for (i = 0; i < optimizerPlans.size(); i++) {
            System.out.println("------------------------------");
            System.out.println("Processing statement " + (i + 1) + "\n");
            System.out.println("optimizer plan:\n" + optimizerPlans.get(i) + "\n");
            System.out.println("delegate plan:\n " + delegatePlans.get(i) + "\n");
        }
        //}
    }
}
