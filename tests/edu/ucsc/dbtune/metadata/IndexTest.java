package edu.ucsc.dbtune.metadata;

import java.util.List;
import java.util.ArrayList;

import org.junit.Test;
import org.junit.Before;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Catalog;

import static edu.ucsc.dbtune.metadata.Index.CLUSTERED;
import static edu.ucsc.dbtune.metadata.Index.PRIMARY;
import static edu.ucsc.dbtune.metadata.Index.UNIQUE;
import static edu.ucsc.dbtune.metadata.SQLTypes.INTEGER;

import static org.junit.Assert.assertThat;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Test for the Index class
 */
public class IndexTest
{ 
    private static List<Column> columns;
    private static Table table;
    private static Schema schema;
    private static Catalog catalog;

    @Before
    public void setUp() throws Exception
    {
        columns = new ArrayList<Column>();
        catalog = new Catalog( "test_catalog" );
        schema  = new Schema(catalog, "test_schema" );
        table   = new Table(schema, "test_table" );

        for ( int i = 0; i < 10; i++ ) 
        {
            columns.add(new Column(table, "col_" + i, INTEGER));
        }
    }

    @Test
    public void testConstructors() throws Exception
    {
        Index index;

        index = new Index(schema, "testi", PRIMARY, CLUSTERED, UNIQUE);

        assertThat(index.getContainer(),is((DatabaseObject)schema));

        index = new Index("other", columns, PRIMARY, CLUSTERED, UNIQUE);

        assertThat(index.getContainer(), is((DatabaseObject)schema));
        assertThat(index.size(), is(table.size()));
    }

    @Test
    public void testEquality() throws Exception
    {
        Index index1;
        Index index2;
        Index index3;

        index1 = new Index( "index1", columns, PRIMARY, CLUSTERED, UNIQUE );
        index2 = new Index( "index2", columns, PRIMARY, CLUSTERED, UNIQUE );
        index3 = new Index( schema, "testi", PRIMARY, CLUSTERED, UNIQUE );

        int i = 0;
        for ( Column col : table.columns() )
        {
            if ( i % 2 == 0 )
            {
                index3.add( col );
            }
            i++;
        }

        assertThat(index1, is(not(index2)));
        assertThat(index2, is(not(index1)));
        assertThat(index1, is(not(index3)));
        assertThat(index2, is(not(index3)));
    }

    @Test
    public void testHashing() throws Exception
    {
        Index index1;
        Index index2;
        Index index3;

        index1 = new Index( "index1", columns, PRIMARY, CLUSTERED, UNIQUE );
        index2 = new Index( "index2", columns, PRIMARY, CLUSTERED, UNIQUE );
        index3 = new Index( schema, "testi", PRIMARY, CLUSTERED, UNIQUE );

        int i = 0;
        for ( Column col : table.columns() )
        {
            if ( i % 2 == 0 )
            {
                index3.add( col );
            }
            i++;
        }


        assertThat(index1.hashCode(), is(not(index3.hashCode())));
        assertThat(index1.hashCode(), is(not(index2.hashCode())));
    }
}
