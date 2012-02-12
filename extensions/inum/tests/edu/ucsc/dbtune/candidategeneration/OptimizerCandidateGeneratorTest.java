package edu.ucsc.dbtune.candidategeneration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import edu.ucsc.dbtune.metadata.Index;

import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.util.TestUtils.randomSubset;

import static org.hamcrest.Matchers.is;

import static org.junit.Assert.assertThat;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Ivo Jimenez
 */
public class OptimizerCandidateGeneratorTest
{
    /**
     * @throws Exception
     *      if error
     */
    @Test
    public void testBasicUsage() throws Exception
    {
        Optimizer opt = mock(Optimizer.class);
        Workload wl = mock(Workload.class);
        List<SQLStatement> stmts = new ArrayList<SQLStatement>();
        Set<Index> a = Sets.newHashSet(Iterables.filter(configureCatalog().getAll(), Index.class));
        Set<Index> actuallyConsidered = new HashSet<Index>();

        for (int i = 0; i < 100; i++) {

            final SQLStatement stmt = new SQLStatement("SELECT " + i);

            stmts.add(stmt);
            Set<Index> subsetOfAll = randomSubset(a, 3);
            when(opt.recommendIndexes(stmt)).thenReturn(subsetOfAll);
            actuallyConsidered.addAll(subsetOfAll);
        }

        when(wl.iterator()).thenReturn(stmts.iterator());

        OptimizerCandidateGenerator cg = new OptimizerCandidateGenerator(opt);
        assertThat(cg.generate(wl).size(), is(actuallyConsidered.size()));
    }
}
