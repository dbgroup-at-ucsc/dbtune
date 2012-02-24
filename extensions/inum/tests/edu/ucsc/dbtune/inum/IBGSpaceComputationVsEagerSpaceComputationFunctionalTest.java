package edu.ucsc.dbtune.inum;

import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.InumPlanSetWithCache;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
import static edu.ucsc.dbtune.util.TestUtils.workloads;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Ivo Jimenez
 */
public class IBGSpaceComputationVsEagerSpaceComputationFunctionalTest
{
    private static DatabaseSystem db;
    private static Environment env;
    private static Optimizer delegate;
    private static CandidateGenerator candGen;
    private static InumSpaceComputation ibgComputation;
    private static InumSpaceComputation eagerComputation;

    /**
     * @throws Exception
     *      if {@link #newDatabaseSystem} throws an exception
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        env = Environment.getInstance();
        db = newDatabaseSystem(env);
        delegate = getBaseOptimizer(db.getOptimizer());
        candGen = CandidateGenerator.Factory.newCandidateGenerator(env, db.getOptimizer());
        ibgComputation = new IBGSpaceComputation();
        eagerComputation = new EagerSpaceComputation();
        
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
    public void testComparison() throws Exception
    {
        //for (Workload wl : workloads(env.getWorkloadFolders())) {

        Workload wl = workload(env.getWorkloadsFoldername() + "/tpch-small");

            System.out.println("==========================================");
            System.out.println("Processing workload " + wl.getName() + "\n");

            final Set<Index> conf = candGen.generate(wl);

            System.out.println("Candidates generated: " + conf.size());

            for (SQLStatement sql : wl) {

                Set<InumPlan> inumSpaceIBG = new InumPlanSetWithCache();
                Set<InumPlan> inumSpaceEager = new InumPlanSetWithCache();

                ibgComputation.compute(inumSpaceIBG, sql, delegate, db.getCatalog());
                eagerComputation.compute(inumSpaceEager, sql, delegate, db.getCatalog());

                //assertThat("For query " + sql, inumSpaceIBG.size(), is(inumSpaceEager.size()));

                /*
                for (InumPlan template : inumSpaceEager)
                    assertThat("For query " + sql, inumSpaceIBG.contains(template), is(true));
                    */
            }
        //}
    }
}
