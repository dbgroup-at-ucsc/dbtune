package edu.ucsc.dbtune.util;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;

import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests of IBG nodes.
 *
 * @author Ivo Jimenez
 */
public class IndexBitSetTest
{
    /**
     * Test that an index bitset is constructed correctly.
     * 
     * @throws Exception
     *     if the catalog can't be configured properly.
     */
    @Test
    public void testBasicUsage() throws Exception
    {
        Catalog       cat  = configureCatalog();
        Configuration conf = new Configuration(((Schema) cat.at(0)).indexes());
        IndexBitSet   bs   = new IndexBitSet();

        for (Index idx : conf)
            bs.add(conf.getOrdinalPosition(idx));

        for (Index idx : conf)
            assertThat(bs.contains(conf.getOrdinalPosition(idx)), is(true));

        IndexBitSet other = new IndexBitSet(bs);

        assertThat(bs, is(other));

        other = new IndexBitSet();

        other.add(1);
        other.add(3);
        other.add(5);
        other.add(7);

        assertThat(bs.containsAll(other), is(true));

        bs = new IndexBitSet();

        bs.addAll(other);

        assertThat(bs,is(other));
    }
}
