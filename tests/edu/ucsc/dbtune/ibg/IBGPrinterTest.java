package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.DatabaseTable;
import edu.ucsc.dbtune.core.IndexExtractor;
import edu.ucsc.dbtune.ibg.CandidatePool.Node;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGChild;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.IndexBitSet;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

import static edu.ucsc.dbtune.core.DBTuneInstances.makeIBGNode;
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
        final String basicQuery = "Select * from R;";
        final DatabaseTable table = mock(DatabaseTable.class);

        // configure indexex

        // duplicate index
        final DBIndex twin = mock(DBIndex.class);
        when(twin.internalId()).thenReturn(1);
        when(twin.creationCost()).thenReturn(22.3);
        when(twin.megabytes()).thenReturn(2.0);
        when(twin.isOn(table)).thenReturn(true);
        when(twin.baseTable()).thenReturn(table);

        final DBIndex soleIndex = mock(DBIndex.class);
        when(soleIndex.internalId()).thenReturn(1);
        when(soleIndex.creationCost()).thenReturn(22.3);
        when(soleIndex.megabytes()).thenReturn(2.0);
        when(soleIndex.isOn(table)).thenReturn(true);
        when(soleIndex.baseTable()).thenReturn(table);
        when(soleIndex.consDuplicate(1)).thenReturn(twin);



        final Iterable<DBIndex> recommendedIndexes = new ArrayList<DBIndex>(Arrays.asList(soleIndex));
        final IndexExtractor extractor = mock(IndexExtractor.class);
        when(extractor.recommendIndexes(basicQuery)).thenReturn(recommendedIndexes);

        final CandidatePool<DBIndex> candidatePool = (CandidatePool<DBIndex>) mock(CandidatePool.class);
        final Node<DBIndex>          root          = (Node<DBIndex>) mock(Node.class);
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

        final DatabaseConnection db = mock(DatabaseConnection.class);
        when(db.getIndexExtractor()).thenReturn(extractor);

    }
}