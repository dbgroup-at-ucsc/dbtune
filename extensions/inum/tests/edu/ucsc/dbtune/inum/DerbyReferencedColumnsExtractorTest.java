package edu.ucsc.dbtune.inum;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.metadata.Index.ASCENDING;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;


import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.workload.SQLStatement;

public class DerbyReferencedColumnsExtractorTest 
{
    private static DerbyReferencedColumnsExtractor extractor;
    private static Catalog catalog;

    /**
     * Setup for the test.
     */
    @BeforeClass
    public static void beforeClassSetUp()
    {
        catalog = configureCatalog();
        extractor = new DerbyReferencedColumnsExtractor(catalog, ASCENDING);
    }

    /**
     * @throws Exception
     *      when an error occurs
     */
    @Test
    public void testSupported() throws Exception
    {  
        SQLStatement sql;
        
        Column c0 = catalog.<Column>findByName("schema_0.table_0.column_0");
        Column c1 = catalog.<Column>findByName("schema_0.table_0.column_1");
        Column c2 = catalog.<Column>findByName("schema_0.table_0.column_2");
        
        sql = new SQLStatement(
                "SELECT " +
                "     column_0, column_2 " +
                "  FROM " +
                "     schema_0.table_0 " +
                "  WHERE " +
                "     column_0 = 5 " +
                "  ORDER BY " +
                "     column_0 ");

        Map<Column, Boolean> map = extractor.getReferencedColumn(sql);
        assertThat(map.containsKey(c0), is(true));
        assertThat(map.containsKey(c2), is(true));
        
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
                "     column_0, column_1"
                ); 
        map = extractor.getReferencedColumn(sql);
        assertThat(map.containsKey(c0), is(true));
        assertThat(map.containsKey(c1), is(true));
    }

}
