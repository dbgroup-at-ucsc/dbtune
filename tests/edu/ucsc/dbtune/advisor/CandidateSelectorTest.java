package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.metadata.PGIndex;
import org.junit.Test;

import java.util.Arrays;

import static edu.ucsc.dbtune.core.DBTuneInstances.generateColumns;
import static edu.ucsc.dbtune.core.DBTuneInstances.generateDescVals;
import static edu.ucsc.dbtune.core.DBTuneInstances.newPGIndex;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class CandidateSelectorTest {
    @Test
    public void testCreationCostOfNewIndex() throws Exception {
        final CandidatesSelector<PGIndex> candidatesSelector = new CandidatesSelector<PGIndex>(40,12345,10,100);
        final double creationCost = candidatesSelector.create(postgresIndex(234, 4321));
        assertThat(Double.compare(4.5, creationCost), equalTo(0));
    }

    @Test
    public void testDropIndex() throws Exception {
        final CandidatesSelector<PGIndex> candidatesSelector = new CandidatesSelector<PGIndex>(40,12345,10,100);
        final double creationCost = candidatesSelector.create(postgresIndex(234, 4321));
        assertThat(Double.compare(4.5, creationCost), equalTo(0));
        final double dropCost = candidatesSelector.drop(postgresIndex(234, 4321));
        assertThat(Double.compare(0, dropCost), equalTo(0));
    }

    @Test
    public void testBasicUsageCase() throws Exception {
        final DynamicIndexSet<PGIndex> userHotSet = new DynamicIndexSet<PGIndex>(){{postgresIndex(2345, 43521);}};
        final CandidatesSelector<PGIndex> candidatesSelector = new CandidatesSelector<PGIndex>(
                new IndexStatisticsFunction<PGIndex>(100),
                new WorkFunctionAlgorithm<PGIndex>(new IndexPartitions<PGIndex>(new StaticIndexSet<PGIndex>(Arrays.asList(postgresIndex(234, 4321)))),
                    false),
                new StaticIndexSet<PGIndex>(Arrays.asList(postgresIndex(234, 4321))),
                new DynamicIndexSet<PGIndex>(){{add(postgresIndex(234, 4321));}},
                userHotSet,40,12345,100
        );
        candidatesSelector.create(postgresIndex(134, 4321));
        candidatesSelector.negativeVote(postgresIndex(234, 4321));
        assertThat(userHotSet.contains(postgresIndex(2345, 43521)), is(false));
    }

    private static PGIndex postgresIndex(int id, int schemaId){
        return newPGIndex(id, schemaId, generateColumns(3), generateDescVals(3));
    }
}
