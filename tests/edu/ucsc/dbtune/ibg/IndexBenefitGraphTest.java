package edu.ucsc.dbtune.ibg;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.DBTuneInstances.configureIndexBenefitGraph;
import static edu.ucsc.dbtune.DBTuneInstances.configurePowerSet;

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
    private static Catalog cat;

    /**
     * configures the {@link IndexBenefitGraph} under test.
     * @throws Exception
     *      if something goes wrong
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        cat = configureCatalog();
        ibg = configureIndexBenefitGraph(configurePowerSet(cat));
    }

    /**
     * @throws Exception
     *      if something goes wrong
     */
    @Test
    public void testBasic() throws Exception
    {
        Set<Index> bs = new HashSet<Index>();
        List<Index> conf = cat.schemas().get(0).indexes();

        bs = new HashSet<Index>();

        bs.add(conf.get(0));
        bs.add(conf.get(1));
        bs.add(conf.get(2));
        bs.add(conf.get(3));

        IndexBenefitGraph.Node root = new IndexBenefitGraph.Node(bs, 0);

        assertThat(ibg.emptyCost(), is(80.0));

        assertThat(ibg.rootNode().getId(), is(root.getId()));
        assertThat(ibg.rootNode().getConfiguration(), is(root.getConfiguration()));
        // Notes:
        //   * other IBGNode-related operations are covered in IBGNodeTest
        //   * IndexBenefitGraph.find(IndexBitSet) is covered in CoveringNodeFinderTest
    }
}
