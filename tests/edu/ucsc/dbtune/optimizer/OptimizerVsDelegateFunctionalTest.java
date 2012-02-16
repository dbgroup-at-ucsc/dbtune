package edu.ucsc.dbtune.optimizer;

import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
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
        candGen = new OptimizerCandidateGenerator(delegate);
        
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

        //for (Workload wl : workloads(env.getWorkloadFolders())) {
        Workload wl = workload(env.getWorkloadsFoldername() + "/tpch-small");
        final Set<Index> conf = candGen.generate(wl);

        System.out.println("Candidates generated: " + conf.size());

        int i = 1;
        long time;
        long prepareTime;
        long explainTime;
        ExplainedSQLStatement prepared;
        ExplainedSQLStatement explained;

        for (SQLStatement sql : wl) {
            System.out.println("------------------------------");
            System.out.println("Processing statement " + i++ + "\n");

            time = System.currentTimeMillis();

            final PreparedSQLStatement pSql = optimizer.prepareExplain(sql);

            prepareTime = System.currentTimeMillis() - time;
            time = System.currentTimeMillis();

            prepared = pSql.explain(conf);

            explainTime = System.currentTimeMillis() - time;

            explained = delegate.explain(sql, conf);

            System.out.println("optimizer: " + explained.getSelectCost());
            System.out.println("delegate:  " + prepared.getSelectCost());
            System.out.println("prepare:   " + prepareTime + " milliseconds");
            System.out.println("explain:   " + explainTime + " milliseconds\n");
            System.out.println("optimizer plan:\n" + explained.getPlan());
            System.out.println("delegate plan:\n" + prepared.getPlan() + "\n");

            //assertThat(pSql.explain(conf), is(delegate.explain(sql, conf)));
        }
        //}
    }
}
