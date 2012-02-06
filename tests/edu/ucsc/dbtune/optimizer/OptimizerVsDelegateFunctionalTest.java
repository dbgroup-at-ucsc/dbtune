package edu.ucsc.dbtune.optimizer;

import java.io.FileReader;

import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.loadWorkloads;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Functional test for testing non-dbms optimizers against the corresponding dbms one. The optimizer 
 * being tested is specified by the {@link edu.ucsc.dbtune.util.EnvironmentProperties#OPTIMIZER} 
 * property. The base optimizer, i.e. the implementation of {@link Optimizer} that runs right on top 
 * of the DBMS (e.g. {@link DB2Optimizer}) is retrieved through the.
 *
 * @author Ivo Jimenez
 */
public class OptimizerVsDelegateFunctionalTest
{
    private static DatabaseSystem db;
    private static Environment env;
    private static Optimizer optimizer;
    private static Optimizer delegate;

    /**
     * @throws Exception
     *      if {@link #newDatabaseSystem} throws an exception
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        env = Environment.getInstance();
        db = newDatabaseSystem(env);
        optimizer = db.getOptimizer();
        delegate = optimizer.getDelegate();
        
        loadWorkloads(db);
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
    public void testExplain() throws Exception
    {
        Workload wl;

        for (String wlName : env.getWorkloadFolders()) {

            wl = new Workload(new FileReader(wlName + "/workload.sql"));
            
            for (SQLStatement sql : wl)
                assertThat(optimizer.explain(sql), is(delegate.explain(sql)));
        }
    }

    /**
     * @throws Exception
     *      if something goes wrong
     * @see OptimizerTest#checkPreparedExplain
     */
    @Test
    public void testWhatIfExplain() throws Exception
    {
        Workload wl;

        for (String wlName : env.getWorkloadFolders()) {

            wl = new Workload(new FileReader(wlName + "/workload.sql"));
            
            for (SQLStatement sql : wl) {
                final Set<Index> conf = optimizer.recommendIndexes(sql);
                assertThat(optimizer.explain(sql, conf), is(delegate.explain(sql, conf)));
            }
        }
    }

    /**
     * @throws Exception
     *      if something goes wrong
     * @see OptimizerTest#checkPreparedExplain
     */
    @Test
    public void testPreparedSQLStatement() throws Exception
    {
        Workload wl;

        for (String wlName : env.getWorkloadFolders()) {

            wl = new Workload(new FileReader(wlName + "/workload.sql"));
            
            for (SQLStatement sql : wl) {
                final PreparedSQLStatement pSql = optimizer.prepareExplain(sql);
                final Set<Index> conf = optimizer.recommendIndexes(sql);
                assertThat(pSql.explain(conf), is(delegate.explain(sql, conf)));
            }
        }
    }
}
