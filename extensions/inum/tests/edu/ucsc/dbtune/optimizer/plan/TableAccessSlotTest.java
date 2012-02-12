package edu.ucsc.dbtune.optimizer.plan;

import java.sql.SQLException;

import edu.ucsc.dbtune.inum.FullTableScanIndex;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.metadata.Index.ASC;
import static edu.ucsc.dbtune.metadata.Index.DESC;

import static org.hamcrest.Matchers.is;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Ivo Jimenez
 */
public class TableAccessSlotTest
{
    private static Catalog catalog;

    /**
     *
     */
    @BeforeClass
    public static void beforeClass()
    {
        catalog = configureCatalog();
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testConstruction() throws Exception
    {
        TableAccessSlot slot;

        Operator tblScan = new Operator(Operator.TABLE_SCAN, 3000, 1);
        Operator fetch = new Operator(Operator.FETCH, 12000, 1);
        Operator ridScan = new Operator(Operator.RID_SCAN, 8000, 1);
        Operator idxScan = new Operator(Operator.INDEX_SCAN, 5000, 1);
        Operator join = new Operator(Operator.MERGE_SORT_JOIN, 9000, 1);

        Table table = catalog.<Table>findByName("schema_0.table_0");
        Index index = catalog.<Index>findByName("schema_0.table_0_index_2");
        InterestingOrder io = new InterestingOrder(table.columns().get(0), ASC);
        
        tblScan.add(table);
        fetch.add(table);
        ridScan.add(table);
        idxScan.add(index);

        // check a table scan
        try {
            slot = new TableAccessSlot(tblScan, join);
            fail("construction should reject an operator with no columns fetched");
        } catch (SQLException e) {
            assertThat(e.getMessage(), is("No columns fetched for leaf"));
        }

        tblScan.addColumnsFetched(io);

        slot = new TableAccessSlot(tblScan, join);

        assertThat(slot.getTable(), is(table));

        assertThat(
                slot.getIndex(),
                is((Index) FullTableScanIndex.getFullTableScanIndexInstance(table)));
        assertThat(
                slot.isCompatible(FullTableScanIndex.getFullTableScanIndexInstance(table)),
                is(true));

        assertThat(
                slot.isCompatible(FullTableScanIndex.getFullTableScanIndexInstance(table)),
                is(true));

        // check a non-leaf, eg. a join
        try {
            slot = new TableAccessSlot(join, join);
            fail("construction should reject an operator without database objects");
        } catch (SQLException e) {
            assertThat(e.getMessage(), is("Leaf should contain only one object"));
        }

        // check ixScan
        try {
            slot = new TableAccessSlot(idxScan, join);
            fail("construction should reject an operator with no columns fetched");
        } catch (SQLException e) {
            assertThat(e.getMessage(), is("No columns fetched for leaf"));
        }

        idxScan.addColumnsFetched(io);

        slot = new TableAccessSlot(idxScan, join);

        assertThat(slot.getTable(), is(table));

        assertThat(slot.getIndex(), is(index));
        assertThat(slot.isCompatible(index), is(true));

        // check FETCH on top of IDXSCAN
        InterestingOrder io2 = new InterestingOrder(table.columns().get(1), ASC);
        io2.add(table.columns().get(2), DESC);
        io2.add(table.columns().get(3), DESC);

        fetch.addColumnsFetched(io2);

        slot = new TableAccessSlot(idxScan, fetch);

        assertThat(slot.getTable(), is(table));

        assertThat(slot.getIndex(), is(index));
        assertThat(slot.isCompatible(index), is(true));

        assertThat(slot.getColumnsFetched().size(), is(4));
        assertThat(slot.getColumnsFetched().columns(), is(table.columns()));
    }
}
