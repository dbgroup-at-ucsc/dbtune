package edu.ucsc.dbtune.workload;

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
    /** checks the constructor. */
    @Test
    public void testConstructor()
    {
        SQLStatement sql;
        
        sql = new SQLStatement("select * from tbl", SQLCategory.SELECT);
        assertThat(sql.getSQL(), is("select * from tbl"));
        assertThat(sql.getSQLCategory(), is(SQLCategory.SELECT));

        sql = new SQLStatement("select * from tbl");
        assertThat(sql.getSQL(), is("select * from tbl"));
        assertThat(sql.getSQLCategory(), is(SQLCategory.SELECT));

        sql = new SQLStatement("delete from tbl");
        assertThat(sql.getSQL(), is("delete from tbl"));
        assertThat(sql.getSQLCategory(), is(SQLCategory.DELETE));

        sql = new SQLStatement("delete from tbl");
        assertThat(sql.getSQL(), is("delete from tbl"));
        assertThat(sql.getSQLCategory(), is(SQLCategory.DELETE));

        sql = new SQLStatement("io fjasfuuuuuuuuuoduaojk ldfsa l");
        assertThat(sql.getSQL(), is("io fjasfuuuuuuuuuoduaojk ldfsa l"));
        assertThat(sql.getSQLCategory(), is(SQLCategory.UNKNOWN));
    }
}
