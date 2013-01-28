package edu.ucsc.dbtune.optimizer.plan;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.OptimizerUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
import static edu.ucsc.dbtune.util.TestUtils.workloads;

import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;

import static org.hamcrest.Matchers.closeTo;
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
        ExplainedSQLStatement eStmt;
        InumPlan inumPlan;
        Set<Index> conf;
        double costLeafs;

        for (List<SQLStatement> wl : workloads(env.getWorkloadFolders())) {

            System.out.println("Checking workload " + wl.get(0).getWorkload().getWorkloadName());
            
            conf = candGen.generate(wl);

            int i = 0;

            for (SQLStatement sql : wl) {
                i++;

                eStmt = optimizer.explain(sql, conf);

                if (!isPlanSuitableForInum(eStmt.getPlan()))
                    continue;

                costLeafs = 0;
                
                inumPlan = new InumPlan(optimizer, eStmt);

                if (sql.getSQLCategory().isSame(NOT_SELECT)) {
                    assertThat(inumPlan.getSlots().size(), is(1));
                } else {
                    for (Operator l : eStmt.getPlan().leafs())
                        costLeafs += InumPlan.extractCostOfLeafAndRemoveFetch(eStmt.getPlan(), l);

                    assertThat(inumPlan.getSlots().size(), is(eStmt.getPlan().getTables().size()));
                    assertThat(
                            "Failed on stmt " + i + "\n" +
                            "   with template: \n" + inumPlan + "\n" +
                            "   with plan: \n" + eStmt.getPlan(),
                            inumPlan.getInternalCost(),
                            closeTo(eStmt.getSelectCost() - costLeafs, 1.0));
                }

                assertThat(inumPlan.getBaseTableUpdateCost(), is(eStmt.getBaseTableUpdateCost()));
                assertThat(inumPlan.getStatement(), is(eStmt.getStatement()));

                // check the objects being referenced
                assertThat(
                        eStmt.getPlan().getTables().containsAll(inumPlan.getTables()),
                        is(true));
                assertThat(
                        eStmt.getPlan().getIndexes().containsAll(inumPlan.getIndexes()),
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
