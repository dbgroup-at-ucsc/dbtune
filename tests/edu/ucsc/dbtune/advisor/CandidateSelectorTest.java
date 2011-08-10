package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.metadata.Index;

import org.junit.Test;

import java.util.Arrays;

import static edu.ucsc.dbtune.DBTuneInstances.generateColumns;
import static edu.ucsc.dbtune.DBTuneInstances.generateDescVals;
import static edu.ucsc.dbtune.DBTuneInstances.newPGIndex;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class CandidateSelectorTest {
    @Test
    public void testCreationCostOfNewIndex() throws Exception {
        final CandidatesSelector candidatesSelector = new CandidatesSelector(40,12345,10,100);
        final double creationCost = candidatesSelector.create(postgresIndex(234, 4321));
        assertThat(Double.compare(4.5, creationCost), equalTo(0));
    }

    @Test
    public void testDropIndex() throws Exception {
        final CandidatesSelector candidatesSelector = new CandidatesSelector(40,12345,10,100);
        final double creationCost = candidatesSelector.create(postgresIndex(234, 4321));
        assertThat(Double.compare(4.5, creationCost), equalTo(0));
        final double dropCost = candidatesSelector.drop(postgresIndex(234, 4321));
        assertThat(Double.compare(0, dropCost), equalTo(0));
    }

    @Test
    public void testBasicUsageCase() throws Exception {
        final DynamicIndexSet userHotSet = new DynamicIndexSet(){{postgresIndex(2345, 43521);}};
        final CandidatesSelector candidatesSelector = new CandidatesSelector(
                new IndexStatisticsFunction(100),
                new WorkFunctionAlgorithm(new IndexPartitions(new StaticIndexSet(Arrays.asList(postgresIndex(234, 4321)))),
                    false),
                new StaticIndexSet(Arrays.asList(postgresIndex(234, 4321))),
                new DynamicIndexSet(){{add(postgresIndex(234, 4321));}},
                userHotSet,40,12345,100
        );
        candidatesSelector.create(postgresIndex(134, 4321));
        candidatesSelector.negativeVote(postgresIndex(234, 4321));
        assertThat(userHotSet.contains(postgresIndex(2345, 43521)), is(false));
    }

    private static Index postgresIndex(int id, int schemaId) throws Exception {
        return newPGIndex(id, schemaId, generateColumns(3), generateDescVals(3));
    }
}
