package edu.ucsc.dbtune.inum;

import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.InumPlanSetWithCache;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
import static edu.ucsc.dbtune.util.TestUtils.workloads;

import static org.hamcrest.Matchers.is;

import static org.junit.Assert.assertThat;

/**
 * @author Ivo Jimenez
 */
public class GreedyVsExhaustiveMatchingStrategyFunctionalTest
{
    private static DatabaseSystem db;
    private static Environment env;
    private static Optimizer delegate;
    private static CandidateGenerator candGen;
    private static InumSpaceComputation computation;
    private static GreedyMatchingStrategy greedy;
    private static ExhaustiveMatchingStrategy exhaustive;

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
        candGen = CandidateGenerator.Factory.newCandidateGenerator(env, delegate);
        computation = InumSpaceComputation.Factory.newInumSpaceComputation(env);

        greedy = new GreedyMatchingStrategy();
        exhaustive = new ExhaustiveMatchingStrategy();
        
        loadWorkloads(db.getConnection());
    }

    /**
     * @throws Exception
     *      if something goes wrong
     * @see OptimizerTest#checkPreparedExplain
     */
    @Test
    public void testComparison() throws Exception
    {
        SQLStatementPlan planFromGreedy;
        SQLStatementPlan planFromExhaustive;
        long cacheWarmingTime;
        long exhaustiveTime;
        long greedyTime;
        long time;
        int queryNumber;

        System.out.println(
                "workload name, " +
                "query number, " +
                "cache warming time (exhaustive), " +
                "exhaustive time, " +
                "greedy time");

        for (Workload wl : workloads(env.getWorkloadFolders())) {

            Set<Index> conf = candGen.generate(wl);

            queryNumber = 1;

            for (SQLStatement sql : wl) {

                Set<InumPlan> inumSpace = new InumPlanSetWithCache();

                computation.compute(inumSpace, sql, delegate, db.getCatalog());

                time = System.nanoTime();
                exhaustive.match(inumSpace, conf).getInstantiatedPlan();
                cacheWarmingTime = System.nanoTime() - time;

                time = System.nanoTime();
                planFromExhaustive = exhaustive.match(inumSpace, conf).getInstantiatedPlan();
                exhaustiveTime = System.nanoTime() - time;
                
                time = System.nanoTime();
                planFromGreedy = greedy.match(inumSpace, conf).getInstantiatedPlan();
                greedyTime = System.nanoTime() - time;

                assertThat(planFromGreedy, is(planFromExhaustive));

                System.out.println(
                        wl.getName() + "," +
                        queryNumber++ + "," +
                        cacheWarmingTime + "," +
                        exhaustiveTime + "," +
                        greedyTime);
            }
        }
    }
}
