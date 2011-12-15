package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.IndexBitSet;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureIndexBenefitGraph;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link IndexBenefitGraph} as a data structure.
 *
 * @author Ivo Jimenez
 */
public class IndexBenefitGraphTest
{
    private static IndexBenefitGraph ibg;

    /**
     * configures the {@link IndexBenefitGraph} under test.
     */
    @BeforeClass
    public static void beforeClass()
    {
        ibg = configureIndexBenefitGraph();
    }

    /**
     */
    @Test
    public void testBasic()
    {
        IndexBitSet bs = new IndexBitSet();

        bs = new IndexBitSet();
        bs.set(0);
        bs.set(1);
        bs.set(2);
        bs.set(3);

        IBGNode root = new IBGNode(bs, 0);

        assertThat(ibg.emptyCost(), is(80.0));

        assertThat(ibg.isUsed(0), is(true));
        assertThat(ibg.isUsed(1), is(true));
        assertThat(ibg.isUsed(2), is(true));
        assertThat(ibg.isUsed(3), is(true));
        assertThat(ibg.isUsed(4), is(false));

        assertThat(ibg.rootNode().getID(), is(root.getID()));
        assertThat(ibg.rootNode().getConfiguration(), is(root.getConfiguration()));
        // Notes:
        //   * other IBGNode-related operations are covered in IBGNodeTest
        //   * IndexBenefitGraph.find(IndexBitSet) is covered in CoveringNodeFinderTest
    }
}
