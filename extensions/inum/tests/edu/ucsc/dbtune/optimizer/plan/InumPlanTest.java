package edu.ucsc.dbtune.optimizer.plan;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;

import static edu.ucsc.dbtune.metadata.Index.ASC;
import static edu.ucsc.dbtune.metadata.Index.DESC;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for InumPlan.
 *
 * @author Ivo Jimenez
 */
public class InumPlanTest
{
    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testOperatorCostExtraction() throws Exception
    {
        SQLStatementPlan sqlPlan;

        Operator tblScan = new Operator(Operator.TABLE_SCAN, 3000, 1);
        Operator fetch = new Operator(Operator.FETCH, 12000, 1);
        Operator ridScan = new Operator(Operator.RID_SCAN, 8000, 1);
        Operator join = new Operator(Operator.MERGE_SORT_JOIN, 9000, 1);
        //Operator idxScan = mock(Operator.class);

        /*
        // check index scan
        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(idxScan)).thenReturn(ridScan);
        assertThat(InumPlan.extractCostOfLeaf(sqlPlan, idxScan), is(idxScan.getAccumulatedCost()));

        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(idxScan)).thenReturn(fetch);
        assertThat(InumPlan.extractCostOfLeaf(sqlPlan, idxScan), is(fetch.getAccumulatedCost()));

        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(idxScan)).thenReturn(join);
        assertThat(InumPlan.extractCostOfLeaf(sqlPlan, idxScan), is(idxScan.getAccumulatedCost()));
        */

        // check table scan
        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(tblScan)).thenReturn(ridScan);
        assertThat(InumPlan.extractCostOfLeaf(sqlPlan, tblScan), is(tblScan.getAccumulatedCost()));

        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(tblScan)).thenReturn(fetch);
        assertThat(InumPlan.extractCostOfLeaf(sqlPlan, tblScan), is(tblScan.getAccumulatedCost()));

        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(tblScan)).thenReturn(join);
        assertThat(InumPlan.extractCostOfLeaf(sqlPlan, tblScan), is(tblScan.getAccumulatedCost()));
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testBuildingOfQueryForUnseenIndex() throws Exception
    {
        Catalog catalog = configureCatalog();

        TableAccessSlot slot = mock(TableAccessSlot.class);

        Table table = catalog.<Table>findByName("schema_0.table_0");
        Index index = catalog.<Index>findByName("schema_0.table_0_index_2");

        when(slot.getTable()).thenReturn(table);
        when(slot.getIndex()).thenReturn(index);

        InterestingOrder io = new InterestingOrder(table.columns().get(0), ASC);

        when(slot.getColumnsFetched()).thenReturn(io);

        SQLStatement sql = InumPlan.buildQueryForUnseenIndex(slot);

        assertThat(sql.getSQL().contains("SELECT"), is(true));
        assertThat(sql.getSQL().contains("FROM " + table.getFullyQualifiedName()), is(true));
        assertThat(sql.getSQL().contains("WHERE"), is(false));
        assertThat(sql.getSQL().contains("ORDER"), is(true));

        for (Column c : index.columns())
            assertThat(
                sql.getSQL().contains(c.getName() + (index.isAscending(c) ? " ASC" : " DESC")),
                is(true));

        io.add(table.columns().get(1), DESC);
        io.add(table.columns().get(2), ASC);
        io.add(table.columns().get(3), DESC);

        List<Predicate> predicates = new ArrayList<Predicate>();

        predicates.add(new Predicate(null, "A > 10"));
        predicates.add(new Predicate(null, "B > 10"));
        predicates.add(new Predicate(null, "C > 10"));

        slot = mock(TableAccessSlot.class);

        when(slot.getTable()).thenReturn(table);
        when(slot.getIndex()).thenReturn(io);
        when(slot.getColumnsFetched()).thenReturn(io);
        when(slot.getPredicates()).thenReturn(predicates);

        sql = InumPlan.buildQueryForUnseenIndex(slot);

        assertThat(sql.getSQL().contains("SELECT"), is(true));
        assertThat(sql.getSQL().contains("FROM " + table.getFullyQualifiedName()), is(true));
        assertThat(sql.getSQL().contains("WHERE"), is(true));
        assertThat(sql.getSQL().contains("ORDER"), is(true));

        for (Column c : io.columns()) {
            assertThat(
                sql.getSQL().contains(c.getName()), is(true));
            assertThat(
                sql.getSQL().contains(c.getName() + (io.isAscending(c) ? " ASC" : " DESC")),
                is(true));
        }

        assertThat(sql.getSQL().contains("A > 10"), is(true));
        assertThat(sql.getSQL().contains("B > 10"), is(true));
        assertThat(sql.getSQL().contains("C > 10"), is(true));
    }
}
