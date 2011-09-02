package edu.ucsc.dbtune.advisor.bc;

import edu.ucsc.dbtune.advisor.StaticIndexSet;
import edu.ucsc.dbtune.advisor.bc.BcTuner;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.PGIndex;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static edu.ucsc.dbtune.DBTuneInstances.generateColumns;
import static edu.ucsc.dbtune.DBTuneInstances.generateDescVals;

import static org.junit.Assert.assertThat;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class BcTunerTest {
    private BcTuner tuner;

    @Before
    public void setUp() throws Exception {
        tuner = new BcTuner(
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
        final Index c = tuner.chooseIndexToCreate();
        assertThat(c, notNullValue());
    }

    @Test
    public void testIndexToDrop() throws Exception {
        final Index c = tuner.chooseIndexToDrop();
        assertThat(c, nullValue());
    }

    @After
    public void tearDown() throws Exception {
        tuner = null;
    }

    private static Index newPGIndex(int indexId, int schemaId, double creationcost) throws Exception {
       return new PGIndex(schemaId, true, generateColumns(3), generateDescVals(3), indexId, 3.0, creationcost, "Create");
    }


    private static Configuration generateSnapshot(int howmany) throws Exception {
        final Configuration c = new Configuration("Conf");
        for(int i = 0; i < howmany; i++){
            c.add(newPGIndex(i, (i % 3 == 0 ? 1234 : 4321), (i == 3 ? -4.5 : 4.5)));
        }

        return c;
    }
}
