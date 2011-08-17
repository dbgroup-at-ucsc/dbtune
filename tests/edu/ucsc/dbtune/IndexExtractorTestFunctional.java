package edu.ucsc.dbtune;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.util.Iterables;
import edu.ucsc.dbtune.util.SQLScriptExecuter;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Condition;
import org.junit.If;
import org.junit.Test;

import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Depends on the 'one_table' workload.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class IndexExtractorTestFunctional
{
    public final static DatabaseSystem db;
    public final static Environment    en;

    static {
        try {       
            en = Environment.getInstance();
            db = new DatabaseSystem();
        } catch(SQLException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        String ddlfilename = en.getScriptAtWorkloadsFolder("one_table/create.sql");
        //SQLScriptExecuter.execute(connection.getJdbcConnection(), ddlfilename);
    }


    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testConnectionIsAlive() throws Exception {
        assertThat(db.getConnection().isClosed(), is(false));
    }

    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testSingleSQLRecommendIndexes() throws Exception {
        Configuration candidates = db.getOptimizer().recommendIndexes(new SQLStatement("select a from tbl where a = 5;"));

        assertThat(candidates, CoreMatchers.<Object>notNullValue());
        assertThat(Iterables.asCollection(candidates).isEmpty(), is(false));

        candidates = db.getOptimizer().recommendIndexes(new SQLStatement("update tbl set a=-1 where a = 5;"));

        assertThat(candidates, CoreMatchers.<Object>notNullValue());
        assertThat(Iterables.asCollection(candidates).isEmpty(), is(false));
    }

    @AfterClass
    public static void tearDown() throws Exception{
        db.getConnection().close();
    }

    @Condition
    public static boolean isDatabaseConnectionAvailable() throws SQLException {
        return !db.getConnection().isClosed();
    }

}
