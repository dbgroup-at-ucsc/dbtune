package edu.ucsc.dbtune.inum;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

        available.add(new EagerSpaceComputation());
        available.add(new IBGSpaceComputation());
        available.add(new NoneMinMaxSpaceComputation());

        for (Set<InumSpaceComputation> twoComputations : combinations(available, 2))
            compare(get(twoComputations, 0), get(twoComputations, 1));
    }

    /**
     * @param one
     *      the computation being tested
     * @param two
     *      another computation being tested
     * @throws Exception
     *      if something goes wrong
     */
    public static void compare(
            InumSpaceComputation one, InumSpaceComputation two)
        throws Exception
    {
        long time;
        long oneTime;
        long twoTime;
        long oneMatchTime;
        long twoMatchTime;
        MatchingStrategy.Result oneResult;
        MatchingStrategy.Result twoResult;
        List<MatchingStrategy.Result> oneResults = new ArrayList<MatchingStrategy.Result>();
        List<MatchingStrategy.Result> twoResults = new ArrayList<MatchingStrategy.Result>();

        System.out.println("-------------------------------------------------------------");
        System.out.println(
            "wlname, stmt, " +
            one.getClass().getName() + " size," +
            two.getClass().getName() + " size," +
            one.getClass().getName() + " computation time," +
            two.getClass().getName() + " computation time," +
            one.getClass().getName() + " cost," +
            two.getClass().getName() + " cost," +
            one.getClass().getName() + " matching time," +
            two.getClass().getName() + " matching time");

        for (Workload wl : workloads(env.getWorkloadFolders())) {

            int i = 0;

            Set<Index> allIndexes = candGen.generate(wl);

            for (SQLStatement sql : wl) {
                i++;

                Set<InumPlan> inumSpaceOne = new HashSet<InumPlan>();
                Set<InumPlan> inumSpaceTwo = new HashSet<InumPlan>();

                time = System.currentTimeMillis();
                one.compute(inumSpaceOne, sql, delegate, db.getCatalog());
                oneTime = System.currentTimeMillis() - time;
                time = System.currentTimeMillis();
                two.compute(inumSpaceTwo, sql, delegate, db.getCatalog());
                twoTime = System.currentTimeMillis() - time;
                time = System.currentTimeMillis();
                oneResult = matching.match(inumSpaceOne, allIndexes);
                oneMatchTime = System.currentTimeMillis() - time;
                time = System.currentTimeMillis();
                twoResult = matching.match(inumSpaceTwo, allIndexes);
                twoMatchTime = System.currentTimeMillis() - time;

                System.out.println(
                        wl.getName() + "," +
                        i + "," +
                        inumSpaceOne.size() + "," +
                        inumSpaceTwo.size() + "," +
                        oneTime + "," +
                        twoTime + "," +
                        oneResult.getBestCost() + "," +
                        twoResult.getBestCost() + "," +
                        oneMatchTime + "," +
                        twoMatchTime);

                oneResults.add(oneResult);
                twoResults.add(twoResult);

                assertThat(
                        "Workload: " + wl.getName() + "\n" +
                        "Statement: " + i + "\n" +
                        "SpaceComputation one: " + one.getClass().getName() + "\n" +
                        "SpaceComputation one: " + two.getClass().getName() + "\n" +
                        "Template one:\n" + oneResult.getBestTemplate() + "\n" +
                        "Template two:\n" + twoResult.getBestTemplate() + "\n" +
                        "Instantiated plan one:\n" + oneResult.getInstantiatedPlan() + "\n" +
                        "Instantiated plan two:\n" + twoResult.getInstantiatedPlan(),
                        oneResult.getBestCost(), closeTo(twoResult.getBestCost(), 1.0));
            }

            /*
            System.out.println("-------------------------------------------------------------");
            System.out.println("Plans");
            System.out.println("-------------------------------------------------------------");
            for (int j = 0; j < oneResults.size(); j++) {
                System.out.println(
                        "One Result for statement " + (j + 1) + ":\n" + oneResults.get(j));
                System.out.println(
                        "Two Result for statement " + (j + 1) + ":\n" + twoResults.get(j));
            }
            */
        }
    }
}
