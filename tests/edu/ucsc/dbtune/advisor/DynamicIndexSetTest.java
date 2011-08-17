package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.DBTuneInstances;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.PGIndex;
import edu.ucsc.dbtune.util.Instances;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class DynamicIndexSetTest {
    @Test
    public void testDynamicIndexSetTestCreation() throws Exception {
        final DynamicIndexSet idxset = new DynamicIndexSet();
        populateIndexSet(idxset, 1000, true);
        assertThat(idxset.isEmpty(), is(false));
        assertThat(idxset.size(), equalTo(1000));
        assertThat(idxset.size(), equalTo(1000));
    }

    @Test
    public void testAddRemove() throws Exception {
        final DynamicIndexSet idxset = new DynamicIndexSet();
        idxset.add(postgresIndex());
        assertThat(idxset.size(), equalTo(1));
        idxset.remove(postgresIndex());
        assertThat(idxset.isEmpty(), is(true));
    }

    @Test
    public void testWeirdSideEffect() throws Exception {
        final DynamicIndexSet idxset = new DynamicIndexSet();
        populateIndexSet(idxset, 1000, true);
        for(Index each : idxset){each.getId();}
        boolean again = false;
        for(Index each : idxset){
        	each.getId();
            again |= true;
        }

        assertThat(again, is(true));
    }

    private static PGIndex postgresIndex() throws Exception {
        final List<Column> cols = Instances.newList();
        final List<Boolean>        desc = Instances.newList();
        return new PGIndex(12, true, cols, desc, 1, 3.0, 4.5, "");
    }

    private void populateIndexSet(DynamicIndexSet idxset, int numberOfElements, boolean postgres) throws Exception {
        for(int idx = 0; idx < numberOfElements; idx++){
            idxset.add(postgres ? DBTuneInstances.newPGIndex(idx) : DBTuneInstances.newDB2Index());
        }
    }
}
