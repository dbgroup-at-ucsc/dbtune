/* **************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                         *
 *                                                                              *
 *   Licensed under the Apache License, Version 2.0 (the "License");            *
 *   you may not use this file except in compliance with the License.           *
 *   You may obtain a copy of the License at                                    *
 *                                                                              *
 *       http://www.apache.org/licenses/LICENSE-2.0                             *
 *                                                                              *
 *   Unless required by applicable law or agreed to in writing, software        *
 *   distributed under the License is distributed on an "AS IS" BASIS,          *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 *   See the License for the specific language governing permissions and        *
 *   limitations under the License.                                             *
 * **************************************************************************** */
package edu.ucsc.dbtune.util;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.util.IndexBitSet;

import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

/**
 * Tests of IBG nodes
 *
 * @author Ivo Jimenez
 */
public class IndexBitSetTest
{
    /**
     * Test that an index bitset is constructed correctly
     */
    @Test
    public void testBasicUsage() throws Exception
    {
        Catalog       cat  = configureCatalog();
        Configuration conf = new Configuration(((Schema) cat.at(0)).indexes());
        IndexBitSet   bs   = new IndexBitSet();

        for (Index idx : conf)
            bs.set(conf.getOrdinalPosition(idx));

        for (Index idx : conf)
            assertThat(bs.get(conf.getOrdinalPosition(idx)), is(true));

        IndexBitSet other = bs.clone();

        assertThat(bs,is(other));

        other = new IndexBitSet();

        other.set(1);
        other.set(3);
        other.set(5);
        other.set(7);

        assertThat(other.subsetOf(bs), is(true));

        bs = new IndexBitSet();

        bs.set(other);

        assertThat(bs,is(other));
    }
}
