package edu.ucsc.dbtune.inum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;

import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.google.common.collect.Iterables.get;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import static edu.ucsc.dbtune.util.MathUtils.combinations;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
import static edu.ucsc.dbtune.util.TestUtils.workloads;

import static org.hamcrest.Matchers.closeTo;

import static org.junit.Assert.assertThat;

/**
 * @author Ivo Jimenez
 */
public class InumSpaceComputationFunctionalTest
{
    private static DatabaseSystem db;
    private static Environment env;
    private static Optimizer delegate;
    private static MatchingStrategy matching;
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
        delegate = getBaseOptimizer(db.getOptimizer());
        matching = MatchingStrategy.Factory.newMatchingStrategy(env);
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
     */
    @Test
    public void testComputation() throws Exception
    {
        List<InumSpaceComputation> available = new ArrayList<InumSpaceComputation>();

        available.add(new ExhaustiveSpaceComputation());
        available.add(new IBGSpaceComputation());

        long time;
        long computationTime;
        long matchTime;
        MatchingStrategy.Result result;
        StringBuilder report = new StringBuilder();

        report.append("wlname, stmt,");

        for (InumSpaceComputation c : available) {
            report.append(c.getClass().getName() + " size,");
            report.append(c.getClass().getName() + " computation time,");
            report.append(c.getClass().getName() + " cost,");
            report.append(c.getClass().getName() + " matching time,");
        }

        report.append("\n");

        for (Workload wl : workloads(env.getWorkloadFolders())) {

            int i = 0;

            Set<Index> allIndexes = candGen.generate(wl);

            for (SQLStatement sql : wl) {
                i++;

                List<Set<InumPlan>> spaces = new ArrayList<Set<InumPlan>>();
                Map<InumSpaceComputation, MatchingStrategy.Result> results =
                    new HashMap<InumSpaceComputation, MatchingStrategy.Result>();

                report.append(wl.getName()).append(",").append(i).append(",");

                for (InumSpaceComputation c : available) {
                    Set<InumPlan> space = new HashSet<InumPlan>();

                    spaces.add(space);

                    time = System.currentTimeMillis();
                    c.compute(space, sql, delegate, db.getCatalog());
                    computationTime = System.currentTimeMillis() - time;

                    report.append(space.size()).append(",").append(computationTime).append(",");

                    time = System.currentTimeMillis();
                    result = matching.match(space, allIndexes);
                    matchTime = System.currentTimeMillis() - time;

                    report.append(result.getBestCost()).append(",").append(matchTime).append(",");
                    results.put(c, result);
                }

                for (Set<InumSpaceComputation> pair : combinations(available, 2)) {
                    InumSpaceComputation one = get(pair, 0);
                    InumSpaceComputation two = get(pair, 1);
                    assertThat(
                        "Workload: " + wl.getName() + "\n" +
                        "Statement: " + i + "\n" +
                        "SpaceComputation one: " + one.getClass().getName() + "\n" +
                        "SpaceComputation two: " + two.getClass().getName() + "\n" +
                        "Template one:\n" + results.get(one).getBestTemplate() + "\n" +
                        "Template two:\n" + results.get(two).getBestTemplate() + "\n" +
                        "Instantiated plan one:\n" + results.get(one).getInstantiatedPlan() + 
                        "\n" +
                        "Instantiated plan two:\n" + results.get(two).getInstantiatedPlan(),
                        results.get(one).getBestCost(),
                        closeTo(
                            results.get(two).getBestCost(), results.get(one).getBestCost() * 0.05));
                }

                report.append("\n");
            }
        }
    }
}
