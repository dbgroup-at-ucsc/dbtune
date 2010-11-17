/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */
package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.core.DBTuneInstances;
import edu.ucsc.dbtune.core.metadata.PGIndex;
import edu.ucsc.dbtune.util.Instances;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class CandidatePoolTest {
    @Test
    public void testPopulatingCandidatePool() throws Exception {
        final CandidatePool<PGIndex> pool = new CandidatePool<PGIndex>();
        pool.addIndexes(populate());
        final CandidatePool.Snapshot<PGIndex> snapshot = pool.getSnapshot();
        assertNotNull("the snapshot is not null", snapshot);
        int counter  = 0;
        for(PGIndex each : snapshot){
            final boolean isOkay = each.internalId() == 1 || each.internalId() == 0;
            assertTrue(isOkay);
            ++counter;
        }

        assertTrue(counter == 2);
    }


    private static Iterable<PGIndex> populate() throws Exception {
        final List<PGIndex> indexes = Instances.newList();
        indexes.add(DBTuneInstances.newPGIndex(true, 1238765, 987));
        indexes.add(DBTuneInstances.newPGIndex(false, 12765, 97));
        return indexes;
    }

    @Test
    public void testIndexInSnapshot() throws Exception {
        final CandidatePool<PGIndex> pool = new CandidatePool<PGIndex>();
        pool.addIndexes(populate());
        final PGIndex indexToBeFound1 = DBTuneInstances.newPGIndex(false, 12765, 97);
        final PGIndex indexToBeFound2 = DBTuneInstances.newPGIndex(true, 1238765, 987);
        assertTrue("index is in pool", pool.contains(indexToBeFound1));
        assertTrue("index is in pool", pool.contains(indexToBeFound2));
    }
}
