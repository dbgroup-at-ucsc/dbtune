package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.DBIndex;
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
    private BcTuner tuner;

    @Before
    public void setUp() throws Exception {
        final DatabaseConnection connection;

        connection  = newPGDatabaseConnectionManager().connect();
        tuner       = new BcTuner(
                connection,
                generateSnapshot(10),
                new StaticIndexSet(
                        Arrays.asList(
                                newPGIndex(987, 678, 4.5),
                                newPGIndex(3, 1234, -4.5)
                        )
                )
        );
    }

    @Test
    public void testIndexToCreate() throws Exception {
        final DBIndex c = tuner.chooseIndexToCreate();
        assertThat(c, CoreMatchers.<Object>notNullValue());
    }

    @Test
    public void testIndexToDrop() throws Exception {
        final DBIndex c = tuner.chooseIndexToDrop();
        assertThat(c, CoreMatchers.<Object>nullValue());
    }

    @After
    public void tearDown() throws Exception {
        tuner = null;
    }

    private static DBIndex newPGIndex(int indexId, int schemaId, double creationcost){
       return new PGIndex(schemaId, true, generateColumns(3), generateDescVals(3), indexId, 3.0, creationcost, "Create");
    }


    private static Snapshot generateSnapshot(int howmany) throws Exception {
        final CandidatePool c = new CandidatePool();
        for(int i = 0; i < howmany; i++){
            c.addIndex(newPGIndex(i, (i % 3 == 0 ? 1234 : 4321), (i == 3 ? -4.5 : 4.5)));
        }

        return c.getSnapshot();
    }
}
