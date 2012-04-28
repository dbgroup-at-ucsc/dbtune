package edu.ucsc.dbtune.ibg;

import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.junit.BeforeClass;
import org.junit.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.DBTuneInstances.configurePowerSet;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link IndexBenefitGraph} as a data structure.
 *
 * @author Ivo Jimenez
 */
public class IndexBenefitGraphConstructorTest
{
    private static Optimizer delegate;
    private static SQLStatement select;
    private static Map<String, Set<Index>> confs;
    private static Catalog cat = configureCatalog();

    /**
     * @throws Exception
     *      if an error occurs
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        final ExplainedSQLStatement explainABCD;
        final ExplainedSQLStatement explainABC;
        final ExplainedSQLStatement explainBCD;
        final ExplainedSQLStatement explainAC;
        final ExplainedSQLStatement explainBC;
        final ExplainedSQLStatement explainCD;
        final ExplainedSQLStatement explainC;
        final ExplainedSQLStatement explainD;
        final ExplainedSQLStatement explainEmpty;

        select = new SQLStatement("SELECT * FROM t");
        confs = configurePowerSet(cat);
        delegate = mock(Optimizer.class);
        explainABCD = mock(ExplainedSQLStatement.class);
        explainABC = mock(ExplainedSQLStatement.class);
        explainBCD = mock(ExplainedSQLStatement.class);
        explainAC = mock(ExplainedSQLStatement.class);
        explainBC = mock(ExplainedSQLStatement.class);
        explainCD = mock(ExplainedSQLStatement.class);
        explainC = mock(ExplainedSQLStatement.class);
        explainD = mock(ExplainedSQLStatement.class);
        explainEmpty = mock(ExplainedSQLStatement.class);

        final ArgumentCaptor<Set> setArg = ArgumentCaptor.forClass(Set.class);

        Answer explainAnswer = new Answer() {
            @Override
			public Object answer(InvocationOnMock aInvocation) throws Exception
            {
                Set<Index> sentConf = (Set<Index>) setArg.getValue();
                if (sentConf.equals(confs.get("abcd")))
                    return explainABCD;
                else if (sentConf.equals(confs.get("abc")))
                    return explainABC;
                else if (sentConf.equals(confs.get("bcd")))
                    return explainBCD;
                else if (sentConf.equals(confs.get("ac")))
                    return explainAC;
                else if (sentConf.equals(confs.get("bc")))
                    return explainBC;
                else if (sentConf.equals(confs.get("cd")))
                    return explainCD;
                else if (sentConf.equals(confs.get("d")))
                    return explainD;
                else
                    return explainEmpty;
            }
        };

        when(delegate.explain(select)).thenReturn(explainEmpty);
        when(delegate.explain(eq(select), setArg.capture())).thenAnswer(explainAnswer);
        when(explainABCD.getUsedConfiguration()).thenReturn(confs.get("ad"));
        when(explainABC.getUsedConfiguration()).thenReturn(confs.get("ab"));
        when(explainBCD.getUsedConfiguration()).thenReturn(confs.get("b"));
        when(explainAC.getUsedConfiguration()).thenReturn(confs.get("empty"));
        when(explainBC.getUsedConfiguration()).thenReturn(confs.get("b"));
        when(explainCD.getUsedConfiguration()).thenReturn(confs.get("cd"));
        when(explainC.getUsedConfiguration()).thenReturn(confs.get("empty"));
        when(explainD.getUsedConfiguration()).thenReturn(confs.get("empty"));
        when(explainABCD.getSelectCost()).thenReturn(20.0);
        when(explainABC.getSelectCost()).thenReturn(45.0);
        when(explainBCD.getSelectCost()).thenReturn(50.0);
        when(explainAC.getSelectCost()).thenReturn(80.0);
        when(explainBC.getSelectCost()).thenReturn(50.0);
        when(explainCD.getSelectCost()).thenReturn(65.0);
        when(explainC.getSelectCost()).thenReturn(80.0);
        when(explainD.getSelectCost()).thenReturn(80.0);
        when(explainEmpty.getSelectCost()).thenReturn(80.0);
    }

    /**
     * Empty, to avoid JUnit complains.
     *
     * @throws Exception
     *      if an error occurs
     */
    @Test
    public void testEmptyCost() throws Exception
    {
        IndexBenefitGraph ibg =
            IndexBenefitGraphConstructor.construct(delegate, select, 80.0, confs.get("abcd"));

        assertThat(ibg.emptyCost(), is(80.0));
    }

    /**
     * Empty, to avoid JUnit complains.
     *
     * @throws Exception
     *      if an error occurs
     */
    @SuppressWarnings({ "unchecked" })
    @Test
    public void testNumberOfWhatIfCalls() throws Exception
    {
        verify(delegate, times(0)).explain(select);
        verify(delegate, times(6)).explain(eq(select), (Set<Index>) anySet());
    }

    /**
     * Empty, to avoid JUnit complains.
     *
     * @throws Exception
     *      if an error occurs
     */
    @Test
    public void testConstructedIBG() throws Exception
    {
        IndexBenefitGraph ibg =
            IndexBenefitGraphConstructor.construct(delegate, select, 80.0, confs.get("abcd"));
        IBGNodeTest.cat = cat;
        IBGNodeTest.ibg = ibg;
        IBGNodeTest.beforeClass();
        IBGNodeTest.checkExpansion();
        IBGNodeTest.checkInternalBitSet();
        IBGNodeTest.checkCostAssignment();
        IBGNodeTest.checkStructure();
        IBGNodeTest.checkEdges();
        IBGNodeTest.checkUsedAndClearIndexes();
        IBGNodeTest.checkContainment();
    }
}
