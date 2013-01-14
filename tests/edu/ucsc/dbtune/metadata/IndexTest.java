package edu.ucsc.dbtune.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;

import static edu.ucsc.dbtune.metadata.Index.ASC;
import static edu.ucsc.dbtune.metadata.Index.CLUSTERED;
import static edu.ucsc.dbtune.metadata.Index.DESC;
import static edu.ucsc.dbtune.metadata.Index.PRIMARY;
import static edu.ucsc.dbtune.metadata.Index.UNIQUE;
import static edu.ucsc.dbtune.metadata.SQLTypes.INTEGER;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

import static org.junit.Assert.assertThat;

/**
 * Test for the Index class.
 *
 * @author Ivo Jimenez
 */
public class IndexTest
{
    // CHECKSTYLE:OFF
    private static List<Column> columns;
    private static Table table;
    private static Schema schema;
    private static Catalog catalog;
    private static Random RANDOM = new Random();

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

        int memId = Index.IN_MEMORY_ID.get();

        index = new Index("other", columns, PRIMARY, CLUSTERED, UNIQUE);

        assertThat(index.getContainer(), is((DatabaseObject)schema));
        assertThat(index.size(), is(table.size()));
        assertThat(index.getId(),is(memId));
    }

    @Test
    public void testEquality() throws Exception
    {
        Index index1;
        Index index2;

        index1 = new Index(columns.get(0), ASC);
        index2 = new Index(columns.get(0), DESC);

        assertThat(index1, is(not(index2)));
        assertThat(index2, is(not(index1)));
        assertThat(index2.getId(), is(greaterThan(index1.getId())));
    }

    @Test
    public void testHashing() throws Exception
    {
        Index index1;
        Index index2;

        index1 = new Index(columns.get(0), ASC);
        index2 = new Index(columns.get(0), DESC);

        assertThat(index1.hashCode(), is(not(index2.hashCode())));
    }

    /**
     * Check covering.
     */
    @Test
    public void testIsCoveredBy()
    {
        Catalog catalog = configureCatalog();

        List<Index> indexes = catalog.schemas().get(0).indexes();

        int numOfIndexesToCheck = RANDOM.nextInt(indexes.size() - 1);

        for (int i = 0; i < numOfIndexesToCheck; i++) {
            Index idx = indexes.get(RANDOM.nextInt(indexes.size() - 1));

            for (Index otherIdx : indexes) {
                if (otherIdx.isCoveredBy(idx)) {
                    assertThat(otherIdx.size(), lessThanOrEqualTo(idx.size()));

                    for (int j = 0; j < otherIdx.size(); j++) {
                        assertThat(otherIdx.columns().get(j), is(idx.columns().get(j)));
                        assertThat(otherIdx.getAscending().get(j), is(idx.getAscending().get(j)));
                    }

                } else {
                    assertThat(
                        otherIdx.size() <= idx.size() ||
                        !(otherIdx.getAscending().size() <= idx.getAscending().size() &&
                        otherIdx.getAscending() != idx.getAscending().subList(0, otherIdx.size() - 1)),
                        is(true));
                }
            }
        }
    }
    // CHECKSTYLE:ON
}
