package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DBTuneInstances;
import edu.ucsc.dbtune.core.DatabaseColumn;
import edu.ucsc.dbtune.core.metadata.PGIndex;
import edu.ucsc.dbtune.core.metadata.PGIndexSchema;
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
        final DynamicIndexSet<PGIndex> idxset = new DynamicIndexSet<PGIndex>();
        populateIndexSet(idxset, 1000, true);
        assertThat(idxset.isEmpty(), is(false));
        assertThat(idxset.size(), equalTo(1000));
        assertThat(idxset.size(), equalTo(1000));
    }

    @Test
    public void testAddRemove() throws Exception {
        final DynamicIndexSet<PGIndex> idxset = new DynamicIndexSet<PGIndex>();
        idxset.add(postgresIndex());
        assertThat(idxset.size(), equalTo(1));
        idxset.remove(postgresIndex());
        assertThat(idxset.isEmpty(), is(true));
    }

    @Test
    public void testWeirdSideEffect() throws Exception {
        final DynamicIndexSet<PGIndex> idxset = new DynamicIndexSet<PGIndex>();
        populateIndexSet(idxset, 1000, true);
        for(PGIndex each : idxset){}
        boolean again = false;
        for(PGIndex each : idxset){
            again |= true;
        }

        assertThat(again, is(true));
    }

    private static PGIndex postgresIndex(){
        final List<DatabaseColumn> cols = Instances.newList();
        final List<Boolean>        desc = Instances.newList();
        return new PGIndex(new PGIndexSchema(12, true, cols, desc), 1, 3.0, 4.5, "");
    }

    private <T extends DBIndex> void populateIndexSet(DynamicIndexSet<T> idxset, int numberOfElements, boolean postgres) {
        for(int idx = 0; idx < numberOfElements; idx++){
            idxset.add((T) (postgres ? DBTuneInstances.newPGIndex(idx) : DBTuneInstances.newDB2Index()));
        }
    }
}
