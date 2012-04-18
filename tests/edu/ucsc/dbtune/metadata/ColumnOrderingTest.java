package edu.ucsc.dbtune.metadata;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import static com.google.common.collect.Iterables.get;

import static edu.ucsc.dbtune.metadata.ColumnOrdering.ANY;
import static edu.ucsc.dbtune.metadata.ColumnOrdering.ASC;
import static edu.ucsc.dbtune.metadata.ColumnOrdering.DESC;
import static edu.ucsc.dbtune.metadata.ColumnOrdering.UNKNOWN;
import static edu.ucsc.dbtune.metadata.SQLTypes.INTEGER;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import static org.junit.Assert.assertThat;

/**
 * Test for the ColumnOrdering class.
 *
 * @author Ivo Jimenez
 */
public class ColumnOrderingTest
{
    private static List<Column> columns;
    private static Table table;
    private static Schema schema;
    private static Catalog catalog;

    /**
     * @throws Exception
     *      if fails
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        columns = new ArrayList<Column>();
        catalog = new Catalog("test_catalog");
        schema  = new Schema(catalog, "test_schema");
        table   = new Table(schema, "test_table");

        for (int i = 0; i < 6; i++) {
            columns.add(new Column(table, "col_" + i, INTEGER));
        }
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testConstructors() throws Exception
    {
        ColumnOrdering co;

        // default values
        co = new ColumnOrdering(columns);

        assertThat(co.getColumns(), is(columns));

        for (Column c : co.getColumns())
            assertThat(co.is(c, ASC), is(true));

        for (Map.Entry<Column, Integer> e : co.getOrderings().entrySet())
            assertThat(e.getValue(), is(ASC));

        // single column
        co = new ColumnOrdering(columns.get(0), ANY);

        assertThat(co.getColumns().size(), is(1));
        assertThat(co.getOrdering(columns.get(0)), is(ANY));
        assertThat(get(co.getOrderings().entrySet(), 0).getValue(), is(ANY));

        // with a list of ordering values
        List<Integer> oVals = new ArrayList<Integer>();

        oVals.add(ASC);
        oVals.add(DESC);
        oVals.add(ANY);
        oVals.add(ASC);
        oVals.add(DESC);
        oVals.add(ANY);

        co = new ColumnOrdering(columns, oVals);

        assertThat(co.getOrdering(columns.get(0)), is(ASC));
        assertThat(co.getOrdering(columns.get(1)), is(DESC));
        assertThat(co.getOrdering(columns.get(2)), is(ANY));
        assertThat(co.getOrdering(columns.get(3)), is(ASC));
        assertThat(co.getOrdering(columns.get(4)), is(DESC));
        assertThat(co.getOrdering(columns.get(5)), is(ANY));

        // with a map
        Map<Column, Integer> oValsMap = new HashMap<Column, Integer>();

        oValsMap.put(columns.get(0), ASC);
        oValsMap.put(columns.get(1), DESC);
        oValsMap.put(columns.get(2), ANY);
        oValsMap.put(columns.get(3), ASC);
        oValsMap.put(columns.get(4), DESC);
        oValsMap.put(columns.get(5), ANY);

        co = new ColumnOrdering(columns, oValsMap);

        assertThat(co.getOrdering(columns.get(0)), is(ASC));
        assertThat(co.getOrdering(columns.get(1)), is(DESC));
        assertThat(co.getOrdering(columns.get(2)), is(ANY));
        assertThat(co.getOrdering(columns.get(3)), is(ASC));
        assertThat(co.getOrdering(columns.get(4)), is(DESC));
        assertThat(co.getOrdering(columns.get(5)), is(ANY));
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testBadConstruction() throws Exception
    {
        try {
            new ColumnOrdering(columns.get(0), UNKNOWN);
        } catch (SQLException ex) {
            // ok, code throw exception
            ex.getMessage();
        }

        try {
            new ColumnOrdering(columns.get(0), 11233);
        } catch (SQLException ex) {
            // ok, code throw exception
            ex.getMessage();
        }

        try {
            new ColumnOrdering(columns, new ArrayList<Integer>());
        } catch (SQLException ex) {
            // ok, code throw exception
            ex.getMessage();
        }

        try {
            new ColumnOrdering(columns, new HashMap<Column, Integer>());
        } catch (SQLException ex) {
            // ok, code throw exception
            ex.getMessage();
        }
    }

    /**
     */
    @Test
    public void testValidation()
    {
        assertThat(ColumnOrdering.isValid(ASC), is(true));
        assertThat(ColumnOrdering.isValid(DESC), is(true));
        assertThat(ColumnOrdering.isValid(ANY), is(true));
        assertThat(ColumnOrdering.isValid(UNKNOWN), is(false));
        assertThat(ColumnOrdering.isValid(103030), is(false));
        assertThat(ColumnOrdering.isValid(-1321), is(false));
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testEquality() throws Exception
    {
        ColumnOrdering co1;
        ColumnOrdering co2;

        co1 = new ColumnOrdering(columns);
        co2 = new ColumnOrdering(columns);

        assertThat(co1, is(co2));

        co2 = new ColumnOrdering(columns.get(0), ANY);

        assertThat(co1, is(not(co2)));
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testHashing() throws Exception
    {
        ColumnOrdering co1;
        ColumnOrdering co2;

        co1 = new ColumnOrdering(columns);
        co2 = new ColumnOrdering(columns);

        assertThat(co1.hashCode(), is(co2.hashCode()));

        co2 = new ColumnOrdering(columns.get(0), ANY);

        assertThat(co1.hashCode(), is(not(co2.hashCode())));
    }

    /**
     * @throws Exception
     *      if fails
    */
    @Test
    public void testCovering() throws Exception
    {
        ColumnOrdering co1;
        ColumnOrdering co2;

        co1 = new ColumnOrdering(columns);
        co2 = new ColumnOrdering(columns);

        assertThat(co1.isCoveredBy(co2), is(true));
        assertThat(co2.isCoveredBy(co1), is(true));
        assertThat(co1.isCoveredByIgnoreOrder(co2), is(true));
        assertThat(co2.isCoveredByIgnoreOrder(co1), is(true));

        co2 = new ColumnOrdering(columns.get(1), ASC);

        assertThat(co1.isCoveredBy(co2), is(false));
        assertThat(co2.isCoveredBy(co1), is(false));
        assertThat(co2.isCoveredByIgnoreOrder(co1), is(true));
        
        co2 = new ColumnOrdering(columns.get(0), ASC);

        assertThat(co1.isCoveredBy(co2), is(false));
        assertThat(co2.isCoveredBy(co1), is(true));
        
        co2 = new ColumnOrdering(columns.get(0), ANY);

        assertThat(co1.isCoveredBy(co2), is(false));
        assertThat(co2.isCoveredBy(co1), is(true));
    }
}
