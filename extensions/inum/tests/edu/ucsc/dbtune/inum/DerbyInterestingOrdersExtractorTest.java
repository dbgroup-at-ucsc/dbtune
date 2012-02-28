package edu.ucsc.dbtune.inum;

import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.plan.InterestingOrder;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;
import static edu.ucsc.dbtune.metadata.Index.ASCENDING;
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
        catalog = configureCatalog();
        extractor = new DerbyInterestingOrdersExtractor(catalog, ASCENDING);
    }

    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testSupported() throws Exception
    {
        Map<Table, Set<InterestingOrder>> indexesPerTable;
        SQLStatement sql;

        Table t = catalog.<Table>findByName("schema_0.table_0");
        Column c = catalog.<Column>findByName("schema_0.table_0.column_0");
        InterestingOrder io = new InterestingOrder(c, ASCENDING);

        sql = new SQLStatement(
                "SELECT " +
                "     column_0 " +
                "  FROM " +
                "     schema_0.table_0 " +
                "  WHERE " +
                "     column_0 = 5 " +
                "  ORDER BY " +
                "     column_0");

        indexesPerTable = extractor.extract(sql);

        // only one table referenced
        assertThat(indexesPerTable.size(), is(1));

        // two interesting orders, column_0 and empty
        assertThat(indexesPerTable.get(t).size(), is(1));
        assertThat(indexesPerTable.get(t).contains(getFullTableScanIndexInstance(t)), is(false));
        assertThat(indexesPerTable.get(t).contains(io), is(true));

        sql = new SQLStatement(
                "SELECT " +
                "     column_0 " +
                "  FROM " +
                "     schema_0.table_0 " +
                "  WHERE " +
                "     column_0 = 5 " +
                "  GROUP BY " +
                "     column_0");

        // only one table referenced
        assertThat(indexesPerTable.size(), is(1));

        // two interesting orders, column_0 (by implying it from GROUP BY) and the empty one
        assertThat(indexesPerTable.get(t).size(), is(1));
        assertThat(indexesPerTable.get(t).contains(getFullTableScanIndexInstance(t)), is(false));
        assertThat(indexesPerTable.get(t).contains(io), is(true));
    }
    
    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testOneRelation() throws Exception
    {
        Map<Table, Set<InterestingOrder>> indexesPerTable;
        SQLStatement sql;

        Table t = catalog.<Table>findByName("schema_0.table_0");

        sql = new SQLStatement(
                "SELECT " +
                "     column_0 " +
                "  FROM " +
                "     schema_0.table_0 " +
                "  WHERE " +
                "     column_0 = 5 ");

        indexesPerTable = extractor.extract(sql);

        // only one table referenced
        assertThat(indexesPerTable.size(), is(1));

        // one interesting order
        assertThat(indexesPerTable.get(t).size(), is(0));
        assertThat(indexesPerTable.get(t).contains(getFullTableScanIndexInstance(t)), is(false));
    }
    
    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testOrderBy() throws Exception
    {
        Map<Table, Set<InterestingOrder>> indexesPerTable;
        SQLStatement sql;

        Table t = catalog.<Table>findByName("schema_0.table_0");
        Column c = catalog.<Column>findByName("schema_0.table_0.column_0");
        InterestingOrder io = new InterestingOrder(c, ASCENDING);

        sql = new SQLStatement(
                "SELECT " +
                "     column_0, column_1 " +
                "  FROM " +
                "     schema_0.table_0 " +
                "  WHERE " +
                "     column_0 = 5 " +
                "  ORDER BY " +
                "     column_0, column_1");

        indexesPerTable = extractor.extract(sql);

        // only one table referenced
        assertThat(indexesPerTable.size(), is(1));

        // two interesting orders, column_0 and empty
        assertThat(indexesPerTable.get(t).size(), is(1));
        assertThat(indexesPerTable.get(t).contains(getFullTableScanIndexInstance(t)), is(false));
        assertThat(indexesPerTable.get(t).contains(io), is(true));
    }
        
    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testOrderByGroupBy() throws Exception
    {
        Map<Table, Set<InterestingOrder>> indexesPerTable;
        SQLStatement sql;

        Table t = catalog.<Table>findByName("schema_0.table_0");
        Column c0 = catalog.<Column>findByName("schema_0.table_0.column_0");
        Column c1 = catalog.<Column>findByName("schema_0.table_0.column_1");
        InterestingOrder io0 = new InterestingOrder(c0, ASCENDING);
        InterestingOrder io1 = new InterestingOrder(c1, ASCENDING);
        
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

        indexesPerTable = extractor.extract(sql);

        // only one table referenced
        assertThat(indexesPerTable.size(), is(1));

        // two interesting orders, column_0 and empty
        assertThat(indexesPerTable.get(t).size(), is(2));
        assertThat(indexesPerTable.get(t).contains(getFullTableScanIndexInstance(t)), is(false));
        assertThat(indexesPerTable.get(t).contains(io0), is(true));
        assertThat(indexesPerTable.get(t).contains(io1), is(true));
    }
    
    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testJoinPredicates() throws Exception
    {
        Map<Table, Set<InterestingOrder>> indexesPerTable;
        SQLStatement sql;

        Table t0 = catalog.<Table>findByName("schema_0.table_0");
        Table t1 = catalog.<Table>findByName("schema_0.table_1");
        Column c0 = catalog.<Column>findByName("schema_0.table_0.column_0");
        Column c1 = catalog.<Column>findByName("schema_0.table_1.column_1");
        InterestingOrder io0 = new InterestingOrder(c0, ASCENDING);
        InterestingOrder io1 = new InterestingOrder(c1, ASCENDING);
        
        sql = new SQLStatement(
                "SELECT " +
                "     table_0.column_0, table_1.column_1 " +
                "  FROM " +
                "     schema_0.table_0, schema_0.table_1 " +
                "  WHERE " +
                "     table_0.column_0 = 5 " +
                "  AND "  +
                "     table_0.column_0 = table_1.column_1 ");
        
        indexesPerTable = extractor.extract(sql);

        // two tables referenced
        assertThat(indexesPerTable.size(), is(2));
        
        // find out the relation that is index by indexsPerTable
        assertThat(indexesPerTable.get(t0).size(), is(1));
        assertThat(indexesPerTable.get(t1).size(), is(1));
        assertThat(indexesPerTable.get(t0).contains(getFullTableScanIndexInstance(t0)), is(false));
        assertThat(indexesPerTable.get(t0).contains(io0), is(true));
        assertThat(indexesPerTable.get(t1).contains(getFullTableScanIndexInstance(t1)), is(false));
        assertThat(indexesPerTable.get(t1).contains(io1), is(true));
    }
}
