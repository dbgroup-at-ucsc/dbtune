package edu.ucsc.dbtune.workload;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for the {@link SQLStatement} class.
 *
 * @author Ivo Jimenez
 */
public class SQLStatementTest
{
    @BeforeClass
    public static void setUp() throws Exception
    {
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
    }

    /** checks the constructor */
    @Test
    public void testConstructor() throws Exception
    {
        SQLStatement sql = new SQLStatement("select * from tbl",SQLCategory.SELECT);

        assertThat(sql.getSQL(),is("select * from tbl"));
    }
}
