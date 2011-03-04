package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGChild;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.IndexBitSet;
import org.junit.Test;

import static edu.ucsc.dbtune.core.DBTuneInstances.makeIBGNode;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class IBGPrinterTest {
    @Test
    public void testExtendedPrintingIndexBenefitGraph() throws Exception {
        final IBGNode root  = makeIBGNode(1);
        root.setCost(20.0);
        final IBGNode first = new IBGNode(new IndexBitSet(){{set(3); set(4);}}, 2);
        first.setCost(40.0);
        root.expand(67.8, new IBGChild(first, 2));

        final IndexBenefitGraph graph   = new IndexBenefitGraph(root, 5.0, new IndexBitSet(){{set(3); set(4);}});
        final IBGPrinter        printer = new IBGPrinter(new IndexBitSet(), new IBGNodeQueue()){
            @Override
            void printExpanded(IndexBenefitGraph ibg, IBGNode node) {
                for (int i = node.config.nextSetBit(0); i >= 0; i = node.config.nextSetBit(i+1)) {
                    assertThat(ibg.isUsed(i), is(true));
                }
            }
        };

        printer.print(graph);
    }
}
