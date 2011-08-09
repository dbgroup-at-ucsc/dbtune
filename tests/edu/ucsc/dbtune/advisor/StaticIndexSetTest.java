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
package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DBTuneInstances;
import edu.ucsc.dbtune.core.metadata.Index;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class StaticIndexSetTest {
    @Test
    public void testBasicScenarioOfQueue() throws Exception {
        final StaticIndexSet sidx1 = new StaticIndexSet(StaticIndexSetTest.populateIndexSet(1000, true));
        final StaticIndexSet sidx2 = new StaticIndexSet(StaticIndexSetTest.populateIndexSet(1, false));

        assertThat(sidx1.isEmpty() && sidx2.isEmpty(), is(false));
        assertThat(sidx1.size(), equalTo(1000));
        assertThat(sidx2.size(), equalTo(1));
    }

    @Test
    public void testExistanceOfIndex() throws Exception {
        final StaticIndexSet indexSet = new StaticIndexSet(StaticIndexSetTest.populateIndexSet(10, true));
        System.out.println(indexSet);
        assertThat(indexSet.contains(DBTuneInstances.newPGIndex(3, 123, DBTuneInstances.generateColumns(3), DBTuneInstances.generateDescVals(3))), is(true));
        assertThat(indexSet.size(), equalTo(10));
    }

    @Test
    public void testWeirdSideEffect() throws Exception {
        final StaticIndexSet indexSet = new StaticIndexSet(StaticIndexSetTest.populateIndexSet(10, true));
        for(Index each : indexSet){}
        boolean again = false;
        for(Index each : indexSet){
            again |= true;
        }

        assertThat(again, is(true));
    }


    private static List<Index> populateIndexSet(int size, boolean postgres) throws Exception {
        final List<Index> o = new ArrayList<Index>();
        for(int idx = 0; idx < size; idx++){
            o.add((Index) (postgres ? DBTuneInstances.newPGIndex(idx, 123, DBTuneInstances.generateColumns(3), DBTuneInstances.generateDescVals(3)) : DBTuneInstances.newDB2Index()));
        }
        return o;
    }
}
