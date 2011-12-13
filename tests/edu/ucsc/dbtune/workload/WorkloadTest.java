package edu.ucsc.dbtune.workload;


import java.io.StringReader;

import org.junit.Test;

import static edu.ucsc.dbtune.workload.SQLCategory.DELETE;
import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;
import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.UPDATE;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for the {@link SQLStatement} class.
 *
 * @author Ivo Jimenez
 */
public class WorkloadTest
{
    /**
     * checks that a character stream is parsed correctly and a set of {@link SQLStatement} objects 
     * is obtained properly.
     * 
     * @throws Exception
     *     if fails
     */
    @Test
    public void testConstructor() throws Exception
    {
        String sqls =
            "SELECT * from a\n" +
            "UPDATE tbl set a = 0 where something = 'foo';\n" +
            "-- comment that shouldn't be take into account tbl set a = 0 where g = 'foo';\n" +
            "      -- this one also    \n" +
            "   UPDATE tbl set a = 0 where something = 'foo'   \n" +
            "INSERT into a values (2,3,4)  \n" +
            "DELETE from a where b = 110\n" +
            "      -- this one also    \n" +
            "SELECT * from a  \n" +
            "    with ss as (SELECT ca_county,d_qoy from tpch)\n" +
            "    INSERT into a values (2,3,4)  \n" +
            "      -- this one also    \n" +
            "with ss as (SELECT ca_county,d_qoy from tpch); \n" +
            "      -- this one also    \n" +
            "    DELETE from a where b = 110\n";

        Workload wl = new Workload(new StringReader(sqls));

        assertThat(wl.get(0).getSQL(), is("SELECT * from a"));
        assertThat(wl.get(1).getSQL(), is("UPDATE tbl set a = 0 where something = 'foo'"));
        assertThat(wl.get(2).getSQL(), is("UPDATE tbl set a = 0 where something = 'foo'"));
        assertThat(wl.get(3).getSQL(), is("INSERT into a values (2,3,4)"));
        assertThat(wl.get(4).getSQL(), is("DELETE from a where b = 110"));
        assertThat(wl.get(5).getSQL(), is("SELECT * from a"));
        assertThat(wl.get(6).getSQL(), is("with ss as (SELECT ca_county,d_qoy from tpch)"));
        assertThat(wl.get(7).getSQL(), is("INSERT into a values (2,3,4)"));
        assertThat(wl.get(8).getSQL(), is("with ss as (SELECT ca_county,d_qoy from tpch)"));
        assertThat(wl.get(9).getSQL(), is("DELETE from a where b = 110"));

        assertThat(wl.get(0).getSQLCategory(), is(SELECT));
        assertThat(wl.get(1).getSQLCategory(), is(UPDATE));
        assertThat(wl.get(2).getSQLCategory(), is(UPDATE));
        assertThat(wl.get(3).getSQLCategory(), is(INSERT));
        assertThat(wl.get(4).getSQLCategory(), is(DELETE));
        assertThat(wl.get(5).getSQLCategory(), is(SELECT));
        assertThat(wl.get(6).getSQLCategory(), is(SELECT));
        assertThat(wl.get(7).getSQLCategory(), is(INSERT));
        assertThat(wl.get(8).getSQLCategory(), is(SELECT));
        assertThat(wl.get(9).getSQLCategory(), is(DELETE));

        assertThat(wl.get(0).getSQLCategory().isSame(SELECT),    is(true));
        assertThat(wl.get(0).getSQLCategory().isSame(DELETE),    is(false));
        assertThat(wl.get(0).getSQLCategory().isSame(INSERT),    is(false));
        assertThat(wl.get(0).getSQLCategory().isSame(UPDATE),    is(false));
        assertThat(wl.get(0).getSQLCategory().isSame(NOT_SELECT), is(false));
        assertThat(wl.get(1).getSQLCategory().isSame(UPDATE),    is(true));
        assertThat(wl.get(1).getSQLCategory().isSame(SELECT),    is(false));
        assertThat(wl.get(1).getSQLCategory().isSame(INSERT),    is(false));
        assertThat(wl.get(1).getSQLCategory().isSame(DELETE),    is(false));
        assertThat(wl.get(1).getSQLCategory().isSame(NOT_SELECT), is(true));
        assertThat(wl.get(3).getSQLCategory().isSame(INSERT),    is(true));
        assertThat(wl.get(3).getSQLCategory().isSame(SELECT),    is(false));
        assertThat(wl.get(3).getSQLCategory().isSame(DELETE),    is(false));
        assertThat(wl.get(3).getSQLCategory().isSame(UPDATE),    is(false));
        assertThat(wl.get(3).getSQLCategory().isSame(NOT_SELECT), is(true));
        assertThat(wl.get(4).getSQLCategory().isSame(DELETE),    is(true));
        assertThat(wl.get(4).getSQLCategory().isSame(SELECT),    is(false));
        assertThat(wl.get(4).getSQLCategory().isSame(INSERT),    is(false));
        assertThat(wl.get(4).getSQLCategory().isSame(UPDATE),    is(false));
        assertThat(wl.get(3).getSQLCategory().isSame(NOT_SELECT), is(true));
    }
}
