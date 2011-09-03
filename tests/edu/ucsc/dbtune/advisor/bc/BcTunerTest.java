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
    }

    @Test
    public void testIndexToCreate() throws Exception {
        /*
        final Index c = tuner.chooseIndexToCreate();
        assertThat(c, notNullValue());
XXX: issue #99
        */
    }

    @Test
    public void testIndexToDrop() throws Exception {
        /*
        final Index c = tuner.chooseIndexToDrop();
        assertThat(c, nullValue());
        */
    }

}
