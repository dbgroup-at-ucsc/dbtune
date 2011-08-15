package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.advisor.CandidateIndexExtractor;
import edu.ucsc.dbtune.ibg.CandidatePool.Node;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGChild;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.metadata.SQLCategory;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

import static edu.ucsc.dbtune.DBTuneInstances.makeIBGNode;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    @SuppressWarnings({"unchecked"})
    @Test
    public void testIndexBenefitGraph() throws Exception{
        final SQLStatement sql = new SQLStatement(SQLCategory.QUERY,"Select * from R;");
        final Table table = mock(Table.class);

        // configure indexex

        // duplicate index
        final Index twin = mock(Index.class);
        when(twin.getId()).thenReturn(1);
        when(twin.getCreationCost()).thenReturn(22.3);
        when(twin.getMegaBytes()).thenReturn(2000000000000l);
        when(twin.getTable()).thenReturn(table);

        final Index soleIndex = mock(Index.class);
        when(soleIndex.getId()).thenReturn(1);
        when(soleIndex.getCreationCost()).thenReturn(22.3);
        when(twin.getMegaBytes()).thenReturn(2000000000000l);
        when(soleIndex.getTable()).thenReturn(table);



        final Iterable<Index> recommendedIndexes = new ArrayList<Index>(Arrays.asList(soleIndex));
        final CandidateIndexExtractor extractor = mock(CandidateIndexExtractor.class);
        when(extractor.recommendIndexes(sql)).thenReturn(recommendedIndexes);

        final CandidatePool candidatePool = (CandidatePool) mock(CandidatePool.class);
        final Node<Index>          root          = (Node<Index>) mock(Node.class);
        when(root.getIndex()).thenReturn(twin);
        when(root.getNext()).thenReturn(root);

        when(candidatePool.getFirstNode()).thenReturn(root);
        final Connection connection = mock(Connection.class);
        final Statement statement  = mock(Statement.class);
        final ResultSet resultSet  = mock(ResultSet.class);

        // recording mock's expected behavior
        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute(anyString())).thenReturn(true);
        when(statement.getResultSet()).thenReturn(resultSet);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);

        when(resultSet.next()).thenReturn(
          true,    // first time .next() will return true,
          false    // second time will return false.
        );
    }
}
