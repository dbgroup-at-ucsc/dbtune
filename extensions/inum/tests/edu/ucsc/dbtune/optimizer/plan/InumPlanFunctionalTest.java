package edu.ucsc.dbtune.optimizer.plan;

import java.util.Set;

import com.google.common.collect.Sets;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
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
 * Test for InumPlan. Assumes that {@link SQLStatementPlan} objects are created appropriately by the 
 * underlying DBMS.
 *
 * @author Ivo Jimenez
 */
public class InumPlanFunctionalTest
{
    private static DatabaseSystem db;
    private static Environment env;
    private static Optimizer optimizer;
    private static CandidateGenerator candGen;

    /**
     * @throws Exception
     *      if {@link DatabaseSystem} instance can't be created
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        env = Environment.getInstance();
        db = newDatabaseSystem(env);
        optimizer = getBaseOptimizer(db.getOptimizer());
        candGen = new OptimizerCandidateGenerator(optimizer);

        loadWorkloads(db.getConnection());
    }

    /**
     * @throws Exception
     *      if connection can't be closed
     */
    @AfterClass
    public static void afterClass() throws Exception
    {
        db.getConnection().close();
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testConstruction() throws Exception
    {
        SQLStatementPlan sqlPlan;
        InumPlan inumPlan;
        Set<Index> conf;
        double costLeafs;

        for (Workload wl : workloads(env.getWorkloadFolders())) {

            System.out.println("==============================");
            System.out.println("Checking workload " + wl.getName());
            System.out.println("==============================");
            
            conf = candGen.generate(wl);

            for (SQLStatement sql : wl) {

                sqlPlan = optimizer.explain(sql, conf).getPlan();

                if (!isPlanSuitableForInum(sqlPlan))
                    continue;

                costLeafs = 0;

                for (Operator l : sqlPlan.leafs())
                    costLeafs += InumPlan.extractCostOfLeafAndRemoveFetch(sqlPlan, l);

                System.out.println("---------------------------");
                System.out.println("   Checking INUM template creation for\n" + sql);
                System.out.println("\n\n    plan\n" + sqlPlan);

                inumPlan = new InumPlan(optimizer, sqlPlan);

                // check the same number of slots
                assertThat(inumPlan.getSlots().size(), is(sqlPlan.getTables().size()));

                // check the internal cost
                assertThat(
                        "Failing Query: " + sql.getSQL() + "\n with plan \n" + sqlPlan,
                        inumPlan.getInternalCost(),
                        is(sqlPlan.getRootElement().getAccumulatedCost() - costLeafs));

                // check the objects being referenced
                assertThat(
                        sqlPlan.getTables().containsAll(inumPlan.getTables()),
                        is(true));
                assertThat(
                        sqlPlan.getIndexes().containsAll(inumPlan.getIndexes()),
                        is(true));
            }
        }
    }

    /**
     * Checks if a plan is suitable for INUM, i.e. if it references any table exactly once.
     *
     * @param plan
     *      the plan that is being checked
     * @return
     *      {@code true} if the plan references any table exactly once; {@code false} otherwise
     */
    private static boolean isPlanSuitableForInum(SQLStatementPlan plan)
    {
        // comparing a Set vs a List; if a table is more than once, the list will contain more
        return Sets.newHashSet(plan.getTables()).size() == plan.getTables().size();
    }
}
