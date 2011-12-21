package edu.ucsc.dbtune;

import java.util.Set;

import edu.ucsc.dbtune.core.InumWhatIfOptimizer;
import edu.ucsc.dbtune.core.InumWhatIfOptimizerImpl;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Objects;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests the {@link InumWhatIfOptimizer} implementation.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumWhatIfOptimizerTest
{
    @Test public void testQueryCostEstimation() throws Exception
    {
        final InumWhatIfOptimizer optimizer = SharedFixtures.configureWhatIfOptimizer();
        double cost = optimizer.estimateCost("SELECT * FROM PERSONS;");
        assertThat(Double.compare(7.0, cost), equalTo(0));
    }

    @Test public void testQueryCostEstimation_NonEmpty_HypotheticalIndexes() throws Exception
    {
        final Set<Index> hypotheticalIndexes = SharedFixtures.configureConfiguration();
        //final InumWhatIfOptimizer optimizer = 
        //SharedFixtures.configureWhatIfOptimizer(hypotheticalIndexes);
        //double cost = optimizer.estimateCost("SELECT * FROM PERSONS;");
        //assertThat(Double.compare(7.0, cost), equalTo(0));
    }

    @Test public void testStoppingInumDirectlyFromOptimizer() throws Exception
    {
        final InumWhatIfOptimizer optimizer = SharedFixtures.configureWhatIfOptimizer();
        final InumWhatIfOptimizerImpl castOptimizer = Objects.cast(optimizer,
                InumWhatIfOptimizerImpl.class);
        castOptimizer.endInum();
        assertThat(castOptimizer.getInum().isEnded(), is(true));
    }
}
