package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Table;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/**
 * Unit test for FullTableScanIndex.
 *
 * @author Ivo Jimenez
 */
public class FullTableScanIndexTest
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
        Table r = catalog.<Table>findByName("schema_0.table_0");
        Table s = catalog.<Table>findByName("schema_0.table_1");
        Table t = catalog.<Table>findByName("schema_0.table_2");

        FullTableScanIndex ftsR = getFullTableScanIndexInstance(r);
        FullTableScanIndex ftsS = getFullTableScanIndexInstance(s);
        FullTableScanIndex ftsT = getFullTableScanIndexInstance(t);

        assertThat(ftsR.getFullyQualifiedName(), is(not(ftsS.getFullyQualifiedName())));
        assertThat(ftsS.getFullyQualifiedName(), is(not(ftsT.getFullyQualifiedName())));
        assertThat(ftsR.hashCode(), is(not(ftsS.hashCode())));
        assertThat(ftsR.hashCode(), is(not(ftsT.hashCode())));
        assertThat(ftsR.getFullyQualifiedName(), is(not(ftsT.getFullyQualifiedName())));

        assertThat(ftsR, is(not(ftsS)));
        assertThat(ftsR, is(not(ftsT)));
        assertThat(ftsS, is(not(ftsR)));
        assertThat(ftsS, is(not(ftsT)));

        FullTableScanIndex ftsT2 = getFullTableScanIndexInstance(t);

        assertThat(ftsT, is(ftsT2));
        assertThat(ftsT.getFullyQualifiedName(), is(ftsT2.getFullyQualifiedName()));
        assertThat(ftsT.hashCode(), is(ftsT2.hashCode()));
    }
}

