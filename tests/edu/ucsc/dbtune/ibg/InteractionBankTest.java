package edu.ucsc.dbtune.ibg;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class InteractionBankTest {
    @Test
    public void testBenefitAssignment() throws Exception {
        final InteractionBank bank = new InteractionBank(1024);
        bank.assignBenefit(1, 56.7);
        assertThat(Double.compare(bank.bestBenefit(1), 56.7), equalTo(0));
    }

    @Test
    public void testInteractionAssignment() throws Exception {
        final InteractionBank      bank    = new InteractionBank(1024);
        bank.assignInteraction(1, 2, 65.7);
        assertThat(Double.compare(bank.interactionLevel(1, 2), 65.7), equalTo(0));
    }
}
