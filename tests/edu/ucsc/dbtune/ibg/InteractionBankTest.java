package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.core.DBTuneInstances;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.spi.core.Console;
import org.junit.Test;

import static edu.ucsc.dbtune.core.DBTuneInstances.generateColumns;
import static edu.ucsc.dbtune.core.DBTuneInstances.generateDescVals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class InteractionBankTest {
    @Test
    public void testBenefitAssignment() throws Exception {
        final InteractionBank      bank    = new InteractionBank(snapshotOfThree());
        bank.assignBenefit(1, 56.7);
        assertThat(Double.compare(bank.bestBenefit(1), 56.7), equalTo(0));
    }

    @Test
    public void testInteractionAssignment() throws Exception {
        final InteractionBank      bank    = new InteractionBank(snapshotOfThree());
        bank.assignInteraction(1, 2, 65.7);
        assertThat(Double.compare(bank.interactionLevel(1, 2), 65.7), equalTo(0));
    }


    private static Snapshot snapshotOfThree() throws Exception {
        return candidatePool(new CandidatePool(), 2, 3).getSnapshot();
    }

    private static CandidatePool candidatePool(CandidatePool pool, int interval, int howmany) throws Exception {
        int count = 0;
        info("Creating " + howmany + " indexes.");
        for(int idx = 0; idx < howmany; idx++){
            if(idx == interval){
                ++count;
                interval = interval * 2;
            }
            pool.addIndex(newPGIndex(idx, 1234 + count));
        }
        info("Finish creating " + howmany + " indexes.");
        return pool;
    }


    private static DBIndex newPGIndex(int indexId, int schemaId){
       return DBTuneInstances.newPGIndex(indexId, schemaId, generateColumns(3), generateDescVals(3));
    }

    private static void info(String message){
        Console.streaming().info(message);
    }
}
