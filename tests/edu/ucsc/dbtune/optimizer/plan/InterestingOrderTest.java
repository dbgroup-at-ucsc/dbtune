package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.metadata.Index.ASCENDING;
import static edu.ucsc.dbtune.metadata.Index.DESCENDING;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/**
 * Unit test for InterestingOrderTest.
 *
 * @author Ivo Jimenez
 */
public class InterestingOrderTest
{
    private static Catalog catalog;

    /**
     * Setup for the test.
     */
    @BeforeClass
    public static void beforeClassSetUp()
    {
        catalog = configureCatalog();
    }

    /**
     * @throws SQLException
     *      if a metadata error occurs
     */
    @Test
    public void testBasic() throws SQLException
    {
        Column a = catalog.<Column>findByName("schema_0.table_0.column_0");
        List<Column> columnLst = new ArrayList<Column>();
        Map<Column, Boolean> ascending = new HashMap<Column, Boolean>();

        columnLst.add(a);
        ascending.put(a, ASCENDING);

        InterestingOrder aAsc1 = new InterestingOrder(a, ASCENDING);
        InterestingOrder aAsc2 = new InterestingOrder(columnLst, ascending);

        assertThat(aAsc1.getFullyQualifiedName(), is(aAsc2.getFullyQualifiedName()));
        assertThat(aAsc1.hashCode(), is(aAsc2.hashCode()));
        assertThat(aAsc1.toString(), is(aAsc2.toString()));
        assertThat(aAsc1, is(aAsc2));

        Column b = catalog.<Column>findByName("schema_0.table_0.column_1");

        columnLst.add(b);
        ascending.put(b, ASCENDING);

        aAsc1 = new InterestingOrder(columnLst, ascending);

        columnLst.clear();
        ascending.clear();

        columnLst.add(a);
        columnLst.add(b);
        ascending.put(a, DESCENDING);
        ascending.put(b, DESCENDING);

        aAsc2 = new InterestingOrder(columnLst, ascending);

        assertThat(aAsc1.getFullyQualifiedName(), is(not(aAsc2.getFullyQualifiedName())));
        assertThat(aAsc1.hashCode(), is(not(aAsc2.hashCode())));
        assertThat(aAsc1.toString(), is(not(aAsc2.toString())));
        assertThat(aAsc1, is(not(aAsc2)));

        columnLst.clear();
        ascending.clear();

        columnLst.add(a);
        columnLst.add(b);
        ascending.put(a, DESCENDING);
        ascending.put(b, ASCENDING);

        aAsc2 = new InterestingOrder(columnLst, ascending);

        assertThat(aAsc1.getFullyQualifiedName(), is(not(aAsc2.getFullyQualifiedName())));
        assertThat(aAsc1.hashCode(), is(not(aAsc2.hashCode())));
        assertThat(aAsc1.toString(), is(not(aAsc2.toString())));
        assertThat(aAsc1, is(not(aAsc2)));

        columnLst.clear();
        ascending.clear();

        columnLst.add(a);
        columnLst.add(b);
        ascending.put(a, ASCENDING);
        ascending.put(b, DESCENDING);

        aAsc2 = new InterestingOrder(columnLst, ascending);

        assertThat(aAsc1.getFullyQualifiedName(), is(not(aAsc2.getFullyQualifiedName())));
        assertThat(aAsc1.hashCode(), is(not(aAsc2.hashCode())));
        assertThat(aAsc1.toString(), is(not(aAsc2.toString())));
        assertThat(aAsc1, is(not(aAsc2)));
    }
}
