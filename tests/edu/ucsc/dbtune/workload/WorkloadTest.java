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
    public void testSingleLine() throws Exception
    {
        String sqls =
            "SELECT * from a;\n" +
            "UPDATE tbl set a = 0 where something = 'foo';\n" +
            "-- comment that shouldn't be take into account tbl set a = 0 where g = 'foo';\n" +
            "      -- this one also    \n" +
            "   UPDATE tbl set a = 0 where something = 'foo';   \n" +
            "INSERT into a values (2,3,4);  \n" +
            "DELETE from a where b = 110;\n" +
            "      -- this one also    \n" +
            "SELECT * from a;  \n" +
            "    with ss as (SELECT ca_county,d_qoy from tpch);\n" +
            "    INSERT into a values (2,3,4);  \n" +
            "      -- this one also    \n" +
            "with ss as (SELECT ca_county,d_qoy from tpch); \n" +
            "      -- this one also;    \n" +
            "    DELETE from a where b = 110;\n";

        Workload wl = new Workload(new StringReader(sqls));

        assertThat(wl.size(), is(10));

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

    /**
     * checks that a character stream containing multi-line statements is parsed correctly.
     * 
     * @throws Exception
     *     if I/O error
     */
    @Test
    public void testMultiLine() throws Exception
    {
        String sqls =
            "--query \n" +
            "select \n" +
            "    l_returnflag, \n" +
            "    l_linestatus, \n" +
            "    sum(l_quantity) as sum_qty, \n" +
            "    sum(l_extendedprice) as sum_base_price, \n" +
            "    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, \n" +
            "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, \n" +
            "    avg(l_quantity) as avg_qty, \n" +
            "    avg(l_extendedprice) as avg_price, \n" +
            "    avg(l_discount) as avg_disc, \n" +
            "    count(*) as count_order \n" +
            "from \n" +
            "    lineitem \n" +
            "where \n" +
            "    l_shipdate <= cast('1998-12-01' as date) - 68 days \n" +
            "group by \n" +
            "    l_returnflag, \n" +
            "    l_linestatus \n" +
            "order by \n" +
            "    l_returnflag, \n" +
            "    l_linestatus; \n" +
            " \n" +
            "--query \n" +
            "select \n" +
            "    s_acctbal, \n" +
            "    s_name, \n" +
            "    n_name, \n" +
            "    p_partkey, \n" +
            "    p_mfgr, \n" +
            "    s_address, \n" +
            "    s_phone, \n" +
            "    s_comment \n" +
            "from \n" +
            "    part, \n" +
            "    supplier, \n" +
            "    partsupp, \n" +
            "    nation, \n" +
            "    region \n" +
            "where \n" +
            "    p_partkey = ps_partkey \n" +
            "    and s_suppkey = ps_suppkey \n" +
            "    and p_size = 38 \n" +
            "    and p_type like '%STEEL' \n" +
            "    and s_nationkey = n_nationkey \n" +
            "    and n_regionkey = r_regionkey \n" +
            "    and r_name = 'ASIA' \n" +
            "    and ps_supplycost = ( \n" +
            "        select \n" +
            "            min(ps_supplycost) \n" +
            "        from \n" +
            "            partsupp, \n" +
            "            supplier, \n" +
            "            nation, \n" +
            "            region \n" +
            "        where \n" +
            "            p_partkey = ps_partkey \n" +
            "            and s_suppkey = ps_suppkey \n" +
            "            and s_nationkey = n_nationkey \n" +
            "            and n_regionkey = r_regionkey \n" +
            "            and r_name = 'ASIA' \n" +
            "    ) \n" +
            "order by \n" +
            "    s_acctbal desc, \n" +
            "    n_name, \n" +
            "    s_name, \n" +
            "    p_partkey; \n" +
            " ; \n " +
            " ; \n " +
            " ; \n " +
            " ; \n " +
            " ; \n " +
            " \n" +
            "--query \n" +
            "select \n" +
            "    l_orderkey, \n" +
            "    sum(l_extendedprice * (1 - l_discount)) as revenue, \n" +
            "    o_orderdate, \n" +
            "    o_shippriority \n" +
            "from \n" +
            "    customer, \n" +
            "    orders, \n" +
            "    lineitem \n" +
            "where \n" +
            "    c_mktsegment = 'FURNITURE' \n" +
            "    and c_custkey = o_custkey \n" +
            "    and l_orderkey = o_orderkey \n" +
            "    and o_orderdate < '1995-03-17' \n" +
            "    and l_shipdate > '1995-03-17' \n" +
            "group by \n" +
            "    l_orderkey, \n" +
            "    o_orderdate, \n" +
            "    o_shippriority \n" +
            "order by \n" +
            "    revenue desc, \n" +
            "    o_orderdate;\n";

        Workload wl = new Workload(new StringReader(sqls));

        assertThat(wl.size(), is(3));

        assertThat(wl.get(0).getSQLCategory(), is(SELECT));
        assertThat(wl.get(1).getSQLCategory(), is(SELECT));
        assertThat(wl.get(2).getSQLCategory(), is(SELECT));

        assertThat(wl.get(0).getSQLCategory().isSame(SELECT), is(true));
        assertThat(wl.get(1).getSQLCategory().isSame(SELECT), is(true));
        assertThat(wl.get(2).getSQLCategory().isSame(SELECT), is(true));
    }
}
