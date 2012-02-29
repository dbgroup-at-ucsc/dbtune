package edu.ucsc.dbtune.inum;

import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.AfterClass;
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
public class IBGSpaceComputationVsEagerSpaceComputationFunctionalTest
{
    private static DatabaseSystem db;
    private static Environment env;
    private static Optimizer delegate;
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
        long time;
        long ibgTime;
        long eagerTime;

        for (Workload wl : workloads(env.getWorkloadFolders())) {

            System.out.println("wlname, stmt, ibg size, eager size, ibg time, eager time");

            int i = 1;

            for (SQLStatement sql : wl) {

                Set<InumPlan> inumSpaceIBG = new HashSet<InumPlan>();
                Set<InumPlan> inumSpaceEager = new HashSet<InumPlan>();

                time = System.currentTimeMillis();
                ibgComputation.compute(inumSpaceIBG, sql, delegate, db.getCatalog());
                ibgTime = System.currentTimeMillis() - time;

                time = System.currentTimeMillis();
                eagerComputation.compute(inumSpaceEager, sql, delegate, db.getCatalog());
                eagerTime = System.currentTimeMillis() - time;

                System.out.println(
                        wl.getName() + "," +
                        i++ + "," +
                        inumSpaceIBG.size() + "," +
                        inumSpaceEager.size() + "," +
                        ibgTime + "," +
                        eagerTime);

                assertThat(inumSpaceIBG.size(), is(inumSpaceEager.size()));

                for (InumPlan template : inumSpaceEager) {
                    assertThat(inumSpaceIBG.contains(template), is(true));
                }
            }
        }
    }
}
