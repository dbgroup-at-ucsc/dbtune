package edu.ucsc.dbtune.advisor.candidategeneration;

import java.util.Iterator;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.WorkloadReader;

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
        WorkloadReader wl = mock(WorkloadReader.class);
        SQLStatement sql = mock(SQLStatement.class);

        @SuppressWarnings("unchecked")
        Iterator<SQLStatement> it = (Iterator<SQLStatement>) mock(Iterator.class);

        when(it.next()).thenReturn(sql);
        when(wl.iterator()).thenReturn(it);
        when(cg.generate(sql)).thenReturn(
                Sets.newHashSet(Iterables.filter(configureCatalog().getAll(), Index.class)));

        OneColumnCandidateGenerator oneColG = new OneColumnCandidateGenerator(cg);

        for (Index idx : oneColG.generate(wl))
            assertThat(idx.size(), is(1));
    }
}
