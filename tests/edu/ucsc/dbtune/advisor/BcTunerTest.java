package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.metadata.PGIndex;
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static edu.ucsc.dbtune.core.DBTuneInstances.generateColumns;
import static edu.ucsc.dbtune.core.DBTuneInstances.generateDescVals;
import static edu.ucsc.dbtune.core.DBTuneInstances.newPGDatabaseConnectionManager;
import static org.junit.Assert.assertThat;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class BcTunerTest {
    private BcTuner<PGIndex> tuner;

    @Before
    public void setUp() throws Exception {
        final DatabaseConnection connection;

        connection  = newPGDatabaseConnectionManager().connect();
        tuner       = new BcTuner<PGIndex>(
                connection,
                generateSnapshot(10),
                new StaticIndexSet<PGIndex>(
                        Arrays.asList(
                                newPGIndex(987, 678, 4.5),
                                newPGIndex(3, 1234, -4.5)
                        )
                )
        );
    }

    @Test
    public void testIndexToCreate() throws Exception {
        final PGIndex c = tuner.chooseIndexToCreate();
        assertThat(c, CoreMatchers.<Object>notNullValue());
    }

    @Test
    public void testIndexToDrop() throws Exception {
        final PGIndex c = tuner.chooseIndexToDrop();
        assertThat(c, CoreMatchers.<Object>nullValue());
    }

    @After
    public void tearDown() throws Exception {
        tuner = null;
    }

    private static PGIndex newPGIndex(int indexId, int schemaId, double creationcost){
       return new PGIndex(schemaId, true, generateColumns(3), generateDescVals(3), indexId, 3.0, creationcost, "Create");
    }


    private static Snapshot<PGIndex> generateSnapshot(int howmany) throws Exception {
        final CandidatePool<PGIndex> c = new CandidatePool<PGIndex>();
        for(int i = 0; i < howmany; i++){
            c.addIndex(newPGIndex(i, (i % 3 == 0 ? 1234 : 4321), (i == 3 ? -4.5 : 4.5)));
        }

        return c.getSnapshot();
    }
}
