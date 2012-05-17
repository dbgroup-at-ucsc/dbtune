package edu.ucsc.dbtune.advisor.db2;

import java.sql.ResultSet;
import java.sql.Statement;

import edu.ucsc.dbtune.DatabaseSystem;

import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.util.Environment;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.OptimizerUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import static org.hamcrest.Matchers.is;

import static org.junit.Assert.assertThat;

/**
 * @author Ivo Jimenez
 */
public class DB2AdvisorFunctionalTest
{
    private static DatabaseSystem db;
    private static Environment env;

    /**
     * @throws Exception
     *      if fails
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        env = Environment.getInstance();
        db  = newDatabaseSystem(env);

        loadWorkloads(db.getConnection());
    }

    /**
     * @throws Exception
     *      if fails
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
    public void testAdvisorProcedureExists() throws Exception
    {
        if (!(getBaseOptimizer(db.getOptimizer()) instanceof DB2Optimizer))
            return;

        Statement stmt = db.getConnection().createStatement();

        ResultSet rs = stmt.executeQuery(
                "SELECT " +
                "     procname " +
                "  FROM " +
                "     sysibm.sysprocedures " +
                "  WHERE " +
                "     procname = 'DESIGN_ADVISOR'");

        assertThat("Procedure DESIGN_ADVISOR doesn't exist", rs.next(), is(true));

        rs.close();
        stmt.close();
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void test() throws Exception
    {
        if (!(getBaseOptimizer(db.getOptimizer()) instanceof DB2Optimizer))
            return;

        DB2Advisor db2advis = new DB2Advisor(db, env.getSpaceBudget());

        db2advis.process(workload(env.getWorkloadsFoldername() + "/tpch-inum"));
        assertThat(db2advis.getRecommendation().isEmpty(), is(false));
    }
}
