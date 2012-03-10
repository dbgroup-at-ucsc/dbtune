package edu.ucsc.dbtune.inum;

import java.util.ArrayList;
import java.util.List;
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

import static com.google.common.collect.Iterables.get;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
//import static edu.ucsc.dbtune.util.EnvironmentProperties.IBG;
import static edu.ucsc.dbtune.util.MathUtils.combinations;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
import static edu.ucsc.dbtune.util.TestUtils.workloads;

import static org.hamcrest.Matchers.is;

import static org.junit.Assert.assertThat;

/**
 * @author Ivo Jimenez
 */
public class MatchingStrategyFunctionalTest
{
    private static DatabaseSystem db;
    private static Environment env;
    private static Optimizer delegate;
    private static CandidateGenerator candGen;
    private static InumSpaceComputation computation;

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

        loadWorkloads(db.getConnection());
    }

    /**
     * @throws Exception
     *      if something goes wrong
     */
    @Test
    public void testMatching() throws Exception
    {
        List<MatchingStrategy> available = new ArrayList<MatchingStrategy>();

        available.add(new GreedyMatchingStrategy());
        available.add(new ExhaustiveMatchingStrategy());

        for (Set<MatchingStrategy> twoMatchings : combinations(available, 2))
            compare(get(twoMatchings, 0), get(twoMatchings, 1));
    }

    /**
     * @param one
     *      the matching being tested
     * @param two
     *      another matching being tested
     * @throws Exception
     *      if something goes wrong
     */
    public static void compare(
            MatchingStrategy one, MatchingStrategy two)
        throws Exception
    {
        SQLStatementPlan planFromOne;
        SQLStatementPlan planFromTwo;
        long cacheWarmingTime;
        long oneTime;
        long twoTime;
        long time;
        int queryNumber;

        System.out.println(
                "workload name, " +
                "query number, " +
                "cache warming time (using exhaustive), " +
                one.getClass().getName() + " time, " +
                two.getClass().getName() + " time");

        for (Workload wl : workloads(env.getWorkloadFolders())) {

            Set<Index> conf = candGen.generate(wl);

            queryNumber = 1;

            for (SQLStatement sql : wl) {

                Set<InumPlan> inumSpace = new InumPlanSetWithCache();

                computation.compute(inumSpace, sql, delegate, db.getCatalog());

                time = System.nanoTime();
                new ExhaustiveMatchingStrategy().match(inumSpace, conf).getInstantiatedPlan();
                cacheWarmingTime = System.nanoTime() - time;

                time = System.nanoTime();
                planFromTwo = two.match(inumSpace, conf).getInstantiatedPlan();
                twoTime = System.nanoTime() - time;

                time = System.nanoTime();
                planFromOne = one.match(inumSpace, conf).getInstantiatedPlan();
                oneTime = System.nanoTime() - time;

                assertThat(
                        "Workload: " + wl.getName() + "\n" +
                        "Statement: " + queryNumber + "\n" +
                        "MatchingStrategy one: " + one.getClass().getName() + "\n" +
                        "MatchingStrategy one: " + two.getClass().getName() + "\n" +
                        "Instantiated plan one:\n" + planFromOne + "\n" +
                        "Instantiated plan two:\n" + planFromTwo,
                        planFromOne, is(planFromTwo));

                System.out.println(
                        wl.getName() + "," +
                        queryNumber++ + "," +
                        cacheWarmingTime + "," +
                        oneTime + "," +
                        twoTime);
            }
        }
    }
}
