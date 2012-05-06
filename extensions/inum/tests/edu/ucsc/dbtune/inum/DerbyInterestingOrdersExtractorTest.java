package edu.ucsc.dbtune.inum;

import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.plan.InterestingOrder;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.junit.BeforeClass;
import org.junit.Test;

import static com.google.common.collect.Iterables.get;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalogWithoutIndexes;
import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;
import static edu.ucsc.dbtune.metadata.Index.ASC;
import static edu.ucsc.dbtune.metadata.Index.DESC;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesPerTable;

import static org.hamcrest.Matchers.is;

import static org.junit.Assert.assertThat;

/**
 * Unit test for DerbyInterestingOrdersExtractor.
 *
 * @author Ivo Jimenez
 * @author Quoc Trung Tran
 */
public class DerbyInterestingOrdersExtractorTest
{
    private static DerbyInterestingOrdersExtractor extractor;
    private static Catalog catalog;

    /**
     * Setup for the test.
     */
    @BeforeClass
    public static void beforeClassSetUp()
    {
        catalog = configureCatalogWithoutIndexes();
        extractor = new DerbyInterestingOrdersExtractor(catalog);
    }

    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testOrderBy() throws Exception
    {
        Map<Table, Set<Index>> ordersPerTable;
        SQLStatement sql;
        InterestingOrder io;

        Table t = catalog.<Table>findByName("schema_0.table_0");
        io = new InterestingOrder(catalog.<Column>findByName("schema_0.table_0.column_0"), ASC);

        sql = new SQLStatement(
                "SELECT " +
                "     column_0 " +
                "  FROM " +
                "     schema_0.table_0 " +
                "  WHERE " +
                "     column_0 = 5 " +
                "  ORDER BY " +
                "     column_0, column_1 DESC, column_2 ASC");

        ordersPerTable = getIndexesPerTable(extractor.extract(sql));

        assertThat(ordersPerTable.size(), is(1));

        assertThat(ordersPerTable.get(t).size(), is(1));
        assertThat(ordersPerTable.get(t).contains(getFullTableScanIndexInstance(t)), is(false));
        assertThat(get(ordersPerTable.get(t), 0).size(), is(1));
        assertThat(ordersPerTable.get(t).contains(io), is(true));

        sql = new SQLStatement(
                "SELECT " +
                "     column_0 " +
                "  FROM " +
                "     schema_0.table_0 " +
                "  WHERE " +
                "     column_0 = 5 " +
                "  ORDER BY " +
                "     column_0 DESC");

        ordersPerTable = getIndexesPerTable(extractor.extract(sql));

        assertThat(ordersPerTable.size(), is(1));

        assertThat(ordersPerTable.get(t).size(), is(1));
        assertThat(ordersPerTable.get(t).contains(getFullTableScanIndexInstance(t)), is(false));
        assertThat(get(ordersPerTable.get(t), 0).size(), is(1));
        assertThat(ordersPerTable.get(t).contains(io), is(false));

        io = new InterestingOrder(catalog.<Column>findByName("schema_0.table_0.column_0"), DESC);

        assertThat(ordersPerTable.get(t).contains(io), is(true));
    }

    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testGroupBy() throws Exception
    {
        Map<Table, Set<Index>> ordersPerTable;
        SQLStatement sql;

        Table t = catalog.<Table>findByName("schema_0.table_0");
        Column c = catalog.<Column>findByName("schema_0.table_0.column_0");
        InterestingOrder io = new InterestingOrder(c, ASC);

        sql = new SQLStatement(
                "SELECT " +
                "     column_0 " +
                "  FROM " +
                "     schema_0.table_0 " +
                "  WHERE " +
                "     column_0 = 5 " +
                "  GROUP BY " +
                "     column_0");

        ordersPerTable = getIndexesPerTable(extractor.extract(sql));

        assertThat(ordersPerTable.size(), is(1));

        assertThat(ordersPerTable.get(t).size(), is(1));
        assertThat(ordersPerTable.get(t).contains(getFullTableScanIndexInstance(t)), is(false));
        assertThat(ordersPerTable.get(t).contains(io), is(true));
    }
    
    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testNoOrderByNoGroupBy() throws Exception
    {
        SQLStatement sql;

        sql = new SQLStatement(
                "SELECT " +
                "     column_0 " +
                "  FROM " +
                "     schema_0.table_0 " +
                "  WHERE " +
                "     column_0 = 5 ");

        assertThat(extractor.extract(sql).size(), is(0));
    }
    
    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testOrderByGroupBy() throws Exception
    {
        Map<Table, Set<Index>> ordersPerTable;
        SQLStatement sql;

        Table t = catalog.<Table>findByName("schema_0.table_0");
        Column c0 = catalog.<Column>findByName("schema_0.table_0.column_0");
        Column c1 = catalog.<Column>findByName("schema_0.table_0.column_1");
        InterestingOrder io0 = new InterestingOrder(c0, ASC);
        InterestingOrder io1 = new InterestingOrder(c1, ASC);
        
        sql = new SQLStatement(
                "SELECT " +
                "     column_0, column_1 " +
                "  FROM " +
                "     schema_0.table_0 " +
                "  WHERE " +
                "     column_0 = 5 " +
                "  GROUP BY "  +
                "     column_0, column_1" +
                "  ORDER BY " +
                "     column_0, column_1");

        ordersPerTable = getIndexesPerTable(extractor.extract(sql));

        assertThat(ordersPerTable.size(), is(1));

        assertThat(ordersPerTable.get(t).size(), is(2));
        assertThat(ordersPerTable.get(t).contains(getFullTableScanIndexInstance(t)), is(false));
        assertThat(ordersPerTable.get(t).contains(io0), is(true));
        assertThat(ordersPerTable.get(t).contains(io1), is(true));
    }
    
    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testJoinPredicates() throws Exception
    {
        Map<Table, Set<Index>> ordersPerTable;
        SQLStatement sql;

        Table t0 = catalog.<Table>findByName("schema_0.table_0");
        Table t1 = catalog.<Table>findByName("schema_0.table_1");
        Table t2 = catalog.<Table>findByName("schema_0.table_2");
        Column c0 = catalog.<Column>findByName("schema_0.table_0.column_0");
        Column c1 = catalog.<Column>findByName("schema_0.table_1.column_1");
        Column c2 = catalog.<Column>findByName("schema_0.table_2.column_2");
        InterestingOrder io0 = new InterestingOrder(c0, ASC);
        InterestingOrder io1 = new InterestingOrder(c1, ASC);
        InterestingOrder io2 = new InterestingOrder(c2, ASC);
        
        sql = new SQLStatement(
                "SELECT " +
                "      table_0.column_0, table_1.column_1 " +
                "  FROM " +
                "      schema_0.table_0, schema_0.table_1 " +
                "  WHERE " +
                "      table_0.column_0 = 5 " +
                "  AND table_0.column_0 = table_1.column_1 ");
        
        ordersPerTable = getIndexesPerTable(extractor.extract(sql));

        assertThat(ordersPerTable.size(), is(2));
        
        assertThat(ordersPerTable.get(t0).size(), is(1));
        assertThat(ordersPerTable.get(t1).size(), is(1));
        assertThat(ordersPerTable.get(t0).contains(getFullTableScanIndexInstance(t0)), is(false));
        assertThat(ordersPerTable.get(t0).contains(io0), is(true));
        assertThat(ordersPerTable.get(t1).contains(getFullTableScanIndexInstance(t1)), is(false));
        assertThat(ordersPerTable.get(t1).contains(io1), is(true));

        sql = new SQLStatement(
                "SELECT " +
                "     table_0.column_0, table_1.column_1 " +
                "  FROM " +
                "     schema_0.table_0 join schema_0.table_1 " +
                "        ON table_0.column_0 = table_1.column_1 " +
                "  WHERE " +
                "     table_0.column_0 = 5 ");
        
        ordersPerTable = getIndexesPerTable(extractor.extract(sql));

        assertThat(ordersPerTable.size(), is(2));

        assertThat(ordersPerTable.get(t0).size(), is(1));
        assertThat(ordersPerTable.get(t1).size(), is(1));
        assertThat(ordersPerTable.get(t0).contains(getFullTableScanIndexInstance(t0)), is(false));
        assertThat(ordersPerTable.get(t0).contains(io0), is(true));
        assertThat(ordersPerTable.get(t1).contains(getFullTableScanIndexInstance(t1)), is(false));
        assertThat(ordersPerTable.get(t1).contains(io1), is(true));

        sql = new SQLStatement(
                "SELECT " +
                "     table_0.column_0, table_1.column_1 " +
                "  FROM " +
                "     schema_0.table_0 " +
                "        join " +
                "     schema_0.table_1 " +
                "           ON table_0.column_0 = table_1.column_1 " +
                "        left outer join " +
                "     schema_0.table_2 " +
                "           ON table_1.column_1 = table_2.column_2 " +
                "  WHERE " +
                "     table_0.column_0 = 5 ");
        
        ordersPerTable = getIndexesPerTable(extractor.extract(sql));

        assertThat(ordersPerTable.size(), is(3));

        assertThat(ordersPerTable.get(t0).size(), is(1));
        assertThat(ordersPerTable.get(t1).size(), is(1));
        assertThat(ordersPerTable.get(t2).size(), is(1));
        assertThat(ordersPerTable.get(t0).contains(getFullTableScanIndexInstance(t0)), is(false));
        assertThat(ordersPerTable.get(t0).contains(io0), is(true));
        assertThat(ordersPerTable.get(t1).contains(getFullTableScanIndexInstance(t1)), is(false));
        assertThat(ordersPerTable.get(t1).contains(io1), is(true));
        assertThat(ordersPerTable.get(t2).contains(getFullTableScanIndexInstance(t1)), is(false));
        assertThat(ordersPerTable.get(t2).contains(io2), is(true));

        sql = new SQLStatement(
                "SELECT " +
                "     table_0.column_0, table_1.column_1 " +
                "  FROM " +
                "     schema_0.table_0 " +
                "        inner join " +
                "     schema_0.table_1 " +
                "           ON table_0.column_0 = table_1.column_1 " +
                "        right outer join " +
                "     schema_0.table_2 " +
                "           ON table_1.column_1 = table_2.column_2 " +
                "  WHERE " +
                "     table_0.column_0 = 5 ");
        
        ordersPerTable = getIndexesPerTable(extractor.extract(sql));

        assertThat(ordersPerTable.size(), is(3));

        assertThat(ordersPerTable.get(t0).size(), is(1));
        assertThat(ordersPerTable.get(t1).size(), is(1));
        assertThat(ordersPerTable.get(t2).size(), is(1));
        assertThat(ordersPerTable.get(t0).contains(getFullTableScanIndexInstance(t0)), is(false));
        assertThat(ordersPerTable.get(t0).contains(io0), is(true));
        assertThat(ordersPerTable.get(t1).contains(getFullTableScanIndexInstance(t1)), is(false));
        assertThat(ordersPerTable.get(t1).contains(io1), is(true));
        assertThat(ordersPerTable.get(t2).contains(getFullTableScanIndexInstance(t1)), is(false));
        assertThat(ordersPerTable.get(t2).contains(io2), is(true));
    }

    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testSubQueries() throws Exception
    {
        Map<Table, Set<Index>> ordersPerTable;
        SQLStatement sql;

        Table t0 = catalog.<Table>findByName("schema_0.table_0");
        Table t1 = catalog.<Table>findByName("schema_0.table_1");
        Column c0 = catalog.<Column>findByName("schema_0.table_0.column_0");
        InterestingOrder io0 = new InterestingOrder(c0, ASC);
        
        sql = new SQLStatement(
                "  SELECT " +
                "      column_0, " +
                "      count(*) as order_count " +
                "  FROM " +
                "      schema_0.table_0 " +
                "  WHERE " +
                "          column_1 >= 1000 " +
                "      and column_1 < 10000 " +
                "      and exists ( " +
                "          SELECT " +
                "              * " +
                "          FROM " +
                "              schema_0.table_1 " +
                "          WHERE " +
                "              column_2 = 3 " +
                "      ) " +
                "  GROUP BY " +
                "      column_0 " +
                "  ORDER BY " +
                "      column_0 ");
        
        assertThat(extractor.extract(sql).size(), is(1));
        ordersPerTable = getIndexesPerTable(extractor.extract(sql));

        assertThat(ordersPerTable.size(), is(1));
        assertThat(ordersPerTable.keySet().contains(t0), is(true));
        assertThat(ordersPerTable.keySet().contains(t1), is(false));
        
        assertThat(ordersPerTable.get(t0).size(), is(1));
        assertThat(ordersPerTable.get(t0).contains(getFullTableScanIndexInstance(t0)), is(false));
        assertThat(ordersPerTable.get(t0).contains(io0), is(true));

        sql = new SQLStatement(
                "  SELECT " +
                "      column_0, " +
                "      count(*) as order_count " +
                "  FROM " +
                "      schema_0.table_0, " +
                "      ( " +
                "          SELECT " +
                "              * " +
                "          FROM " +
                "              schema_0.table_1 " +
                "          WHERE " +
                "              column_2 = 3 " +
                "      ) as t " +
                "  WHERE " +
                "          column_1 >= 1000 " +
                "      and column_1 < 10000 " +
                "  GROUP BY " +
                "      column_0 " +
                "  ORDER BY " +
                "      column_0 ");

        ordersPerTable = getIndexesPerTable(extractor.extract(sql));

        assertThat(ordersPerTable.size(), is(1));
        assertThat(ordersPerTable.keySet().contains(t0), is(true));
        assertThat(ordersPerTable.keySet().contains(t1), is(false));
        
        assertThat(ordersPerTable.get(t0).size(), is(1));
        assertThat(ordersPerTable.get(t0).contains(getFullTableScanIndexInstance(t0)), is(false));
        assertThat(ordersPerTable.get(t0).contains(io0), is(true));
    }

    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testCorrelated() throws Exception
    {
        Map<Table, Set<Index>> ordersPerTable;
        SQLStatement sql;

        Table t0 = catalog.<Table>findByName("schema_0.table_0");
        Table t1 = catalog.<Table>findByName("schema_0.table_1");
        Column c0 = catalog.<Column>findByName("schema_0.table_0.column_1");
        Column c1 = catalog.<Column>findByName("schema_0.table_1.column_2");
        InterestingOrder io0 = new InterestingOrder(c0, ASC);
        InterestingOrder io1 = new InterestingOrder(c1, ASC);
        
        sql = new SQLStatement(
                "  SELECT " +
                "      column_0, " +
                "      count(*) as order_count " +
                "  FROM " +
                "      schema_0.table_0 " +
                "  WHERE " +
                "          column_1 >= 1000 " +
                "      and column_1 < 10000 " +
                "      and exists ( " +
                "          SELECT " +
                "              * " +
                "          FROM " +
                "              schema_0.table_1 " +
                "          WHERE " +
                "                  table_1.column_2 = table_0.column_1 " +
                "              and column_2 < column_3 " +
                "      ) " +
                "  GROUP BY " +
                "      column_1 " +
                "  ORDER BY " +
                "      column_1 ");
        ordersPerTable = getIndexesPerTable(extractor.extract(sql));

        assertThat(ordersPerTable.size(), is(2));
        assertThat(ordersPerTable.keySet().contains(t0), is(true));
        assertThat(ordersPerTable.keySet().contains(t1), is(true));
        
        assertThat(ordersPerTable.get(t0).size(), is(1));
        assertThat(ordersPerTable.get(t1).size(), is(1));
        assertThat(ordersPerTable.get(t0).contains(getFullTableScanIndexInstance(t0)), is(false));
        assertThat(ordersPerTable.get(t0).contains(io0), is(true));
        assertThat(ordersPerTable.get(t1).contains(getFullTableScanIndexInstance(t1)), is(false));
        assertThat(ordersPerTable.get(t1).contains(io1), is(true));

        sql = new SQLStatement(
                "  SELECT " +
                "      column_0, " +
                "      count(*) as order_count " +
                "  FROM " +
                "      schema_0.table_0, " +
                "      ( " +
                "          SELECT " +
                "              * " +
                "          FROM " +
                "              schema_0.table_1 " +
                "          WHERE " +
                "                  table_1.column_2 = table_0.column_1 " +
                "              and column_2 < column_3 " +
                "      ) as t " +
                "  WHERE " +
                "          column_1 >= 1000 " +
                "      and column_1 < 10000 " +
                "  GROUP BY " +
                "      column_1 " +
                "  ORDER BY " +
                "      column_1 ");

        ordersPerTable = getIndexesPerTable(extractor.extract(sql));

        assertThat(ordersPerTable.size(), is(2));
        assertThat(ordersPerTable.keySet().contains(t0), is(true));
        assertThat(ordersPerTable.keySet().contains(t1), is(true));
        
        assertThat(ordersPerTable.get(t0).size(), is(1));
        assertThat(ordersPerTable.get(t1).size(), is(1));
        assertThat(ordersPerTable.get(t0).contains(getFullTableScanIndexInstance(t0)), is(false));
        assertThat(ordersPerTable.get(t0).contains(io0), is(true));
        assertThat(ordersPerTable.get(t1).contains(getFullTableScanIndexInstance(t1)), is(false));
        assertThat(ordersPerTable.get(t1).contains(io1), is(true));
    }

    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testSelectionPredicatesOnColumnsOfTheSameTable() throws Exception
    {
        Map<Table, Set<Index>> ordersPerTable;
        SQLStatement sql;

        Table t1 = catalog.<Table>findByName("schema_0.table_1");
        Column c1 = catalog.<Column>findByName("schema_0.table_1.column_0");
        InterestingOrder io1 = new InterestingOrder(c1, ASC);
        
        sql = new SQLStatement(
                "SELECT " +
                "    * " +
                "FROM " +
                "    schema_0.table_1 " +
                "WHERE " +
                "    column_2 < column_3 " +
                "ORDER BY " +
                "    column_0");
        ordersPerTable = getIndexesPerTable(extractor.extract(sql));

        assertThat(ordersPerTable.size(), is(1));
        assertThat(ordersPerTable.keySet().contains(t1), is(true));
        
        assertThat(ordersPerTable.get(t1).size(), is(1));
        assertThat(ordersPerTable.get(t1).contains(getFullTableScanIndexInstance(t1)), is(false));
        assertThat(ordersPerTable.get(t1).contains(io1), is(true));
    }

    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testSilentFails() throws Exception
    {
        SQLStatement sql;

        sql = new SQLStatement(
                "SELECT " +
                "    sum(column_0 * (1 - column_1)) as revenue, " +
                "    column_3 " +
                " FROM " +
                "    schema_0.table_1 " +
                " WHERE " +
                "    column_2 < column_3 " +
                " ORDER BY " +
                "    sum(column_0 * (1 - column_1)) desc");

        // should be one, but aliasing is not supported yet
        assertThat(extractor.extract(sql).size(), is(0));

        sql = new SQLStatement(
                "SELECT " +
                "    sum(column_0 * (1 - column_1)) as revenue, " +
                "    column_3 " +
                " FROM " +
                "    schema_0.table_1 " +
                " WHERE " +
                "    column_2 < column_3 " +
                " GROUP BY " +
                "    revenue");

        // same as above
        assertThat(extractor.extract(sql).size(), is(0));
    }
}
