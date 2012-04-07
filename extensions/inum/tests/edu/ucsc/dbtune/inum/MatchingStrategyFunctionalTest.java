package edu.ucsc.dbtune.inum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
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
    private static Random r;

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
        r = new Random(System.currentTimeMillis());

        loadWorkloads(db.getConnection());
    }

    /**
     * @throws Exception
     *      if something goes wrong
     */
    @Test
    public void testMatching() throws Exception
    {
        if (!(db.getOptimizer() instanceof InumOptimizer))
            return;
        
        List<MatchingStrategy> available = new ArrayList<MatchingStrategy>();

        available.add(new GreedyMatchingStrategy());
        available.add(new ExhaustiveMatchingStrategy());

        SQLStatementPlan plan;
        StringBuilder report;
        long cacheWarmingTime;
        long time;
        long matchTime;
        int queryNumber;

        report = new StringBuilder();
        report.append("workload name, query number, cache warming time (using exhaustive), ");

        for (MatchingStrategy s : available)
            report.append(s.getClass().getName() + " time,");
        
        for (Workload wl : workloads(env.getWorkloadFolders())) {

            Set<Index> conf = candGen.generate(wl);

            queryNumber = 1;

            for (SQLStatement sql : wl) {

                queryNumber++;

                if (r.nextInt(100) > 5)
                    continue;

                Set<InumPlan> inumSpace = new InumPlanSetWithCache();
                Map<MatchingStrategy, SQLStatementPlan> plans =
                    new HashMap<MatchingStrategy, SQLStatementPlan>();

                computation.compute(inumSpace, sql, delegate, db.getCatalog());

                time = System.nanoTime();
                new ExhaustiveMatchingStrategy().match(inumSpace, conf).getInstantiatedPlan();
                cacheWarmingTime = System.nanoTime() - time;

                report.append(wl.getName() + "," + queryNumber + "," + cacheWarmingTime + ",");

                for (MatchingStrategy s : available) {
                    time = System.nanoTime();
                    plan = s.match(inumSpace, conf).getInstantiatedPlan();
                    matchTime = System.nanoTime() - time;
                    report.append(matchTime + ",");
                    plans.put(s, plan);
                }

                for (Set<MatchingStrategy> pair : combinations(available, 2)) {
                    MatchingStrategy one = get(pair, 0);
                    MatchingStrategy two = get(pair, 1);
                    assertThat(
                            "Workload: " + wl.getName() + "\n" +
                            "Statement: " + queryNumber + "\n" +
                            "MatchingStrategy one: " + one.getClass().getName() + "\n" +
                            "MatchingStrategy one: " + two.getClass().getName() + "\n" +
                            "Instantiated plan one:\n" + plans.get(one) + "\n" +
                            "Instantiated plan two:\n" + plans.get(two),
                            plans.get(one), is(plans.get(two)));
                }

                report.append("\n");
            }
        }
    }
}
