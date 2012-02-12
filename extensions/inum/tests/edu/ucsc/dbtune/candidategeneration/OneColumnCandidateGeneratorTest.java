package edu.ucsc.dbtune.candidategeneration;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;

import static org.hamcrest.Matchers.is;

import static org.junit.Assert.assertThat;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Ivo Jimenez
 */
public class OneColumnCandidateGeneratorTest
{
    /**
     * @throws Exception
     *      if error
     */
    @Test
    public void testBasicUsage() throws Exception
    {
        CandidateGenerator cg = mock(CandidateGenerator.class);
        Workload wl = mock(Workload.class);

        when(cg.generate(wl)).thenReturn(
                Sets.newHashSet(Iterables.filter(configureCatalog().getAll(), Index.class)));

        OneColumnCandidateGenerator oneColG = new OneColumnCandidateGenerator(cg);

        for (Index idx : oneColG.generate(wl))
            assertThat(idx.size(), is(1));
    }
}
