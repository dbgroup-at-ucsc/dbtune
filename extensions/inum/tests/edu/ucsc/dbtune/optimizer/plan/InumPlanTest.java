package edu.ucsc.dbtune.optimizer.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.ColumnOrdering;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.optimizer.plan.InumPlan.buildQueryForUnseenIndex;
import static edu.ucsc.dbtune.optimizer.plan.InumPlan.makeOperatorFromSlot;
import static edu.ucsc.dbtune.optimizer.plan.Operator.INDEX_SCAN;
import static edu.ucsc.dbtune.optimizer.plan.Operator.TABLE_SCAN;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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
    private static Catalog catalog = configureCatalog();

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

        ColumnOrdering io = new ColumnOrdering(table.columns().get(0), ColumnOrdering.ASC);

        when(slot.getColumnsFetched()).thenReturn(io);

        SQLStatement sql = buildQueryForUnseenIndex(slot);

        assertThat(sql.getSQL().contains("SELECT"), is(true));
        assertThat(sql.getSQL().contains("FROM " + table.getFullyQualifiedName()), is(true));
        assertThat(sql.getSQL().contains("WHERE"), is(false));
        assertThat(sql.getSQL().contains("ORDER"), is(true));

        for (Column c : index.columns())
            assertThat(
                sql.getSQL().contains(c.getName() + (index.isAscending(c) ? " ASC" : " DESC")),
                is(true));

        List<Column> columns = new ArrayList<Column>();
        Map<Column, Integer> orderings = new HashMap<Column, Integer>();

        columns.add(table.columns().get(0));
        orderings.put(table.columns().get(0), ColumnOrdering.ASC);
        columns.add(table.columns().get(1));
        orderings.put(table.columns().get(1), ColumnOrdering.DESC);
        columns.add(table.columns().get(2));
        orderings.put(table.columns().get(2), ColumnOrdering.ASC);
        columns.add(table.columns().get(3));
        orderings.put(table.columns().get(3), ColumnOrdering.DESC);

        ColumnOrdering io2 = new ColumnOrdering(columns, orderings);

        List<Predicate> predicates = new ArrayList<Predicate>();

        predicates.add(new Predicate(null, "A > 10"));
        predicates.add(new Predicate(null, "B > 10"));
        predicates.add(new Predicate(null, "C > 10"));
        predicates.add(new Predicate(null, "EXISTS (SELECT RID() FROM FOO)"));

        slot = mock(TableAccessSlot.class);

        when(slot.getTable()).thenReturn(table);
        when(slot.getIndex()).thenReturn(new Index(io2));
        when(slot.getColumnsFetched()).thenReturn(io);
        when(slot.getPredicates()).thenReturn(predicates);

        sql = buildQueryForUnseenIndex(slot);

        assertThat(sql.getSQL().contains("SELECT"), is(true));
        assertThat(sql.getSQL().contains("FROM " + table.getFullyQualifiedName()), is(true));
        assertThat(sql.getSQL().contains("WHERE"), is(true));
        assertThat(sql.getSQL().contains("ORDER"), is(true));
        assertThat(sql.getSQL().contains("RID()"), is(true));

        assertThat(sql.getSQL().contains("A > 10"), is(true));
        assertThat(sql.getSQL().contains("B > 10"), is(true));
        assertThat(sql.getSQL().contains("C > 10"), is(true));
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testMakeOperator() throws Exception
    {
        Operator tblScan = new Operator(TABLE_SCAN, 3000, 1);
        Operator idxScan = new Operator(INDEX_SCAN, 5000, 1);

        Table table = catalog.<Table>findByName("schema_0.table_0");
        Index index = catalog.<Index>findByName("schema_0.table_0_index_2");
        ColumnOrdering io = new ColumnOrdering(table.columns().get(0), ColumnOrdering.ASC);
        
        tblScan.addColumnsFetched(io);
        idxScan.addColumnsFetched(io);
        tblScan.add(table);
        idxScan.add(index);

        TableAccessSlot tblScanSlot = new TableAccessSlot(tblScan);
        TableAccessSlot idxScanSlot = new TableAccessSlot(idxScan);

        assertThat(makeOperatorFromSlot(tblScanSlot), is(tblScan));
        assertThat(makeOperatorFromSlot(tblScanSlot), is(not(idxScan)));
        assertThat(makeOperatorFromSlot(idxScanSlot), is(idxScan));
        assertThat(makeOperatorFromSlot(idxScanSlot), is(not(tblScan)));
    }

    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testOperatorCostExtraction() throws Exception
    {
        /*
        SQLStatementPlan sqlPlan;

        Operator tblScan = new Operator(Operator.TABLE_SCAN, 3000, 1);
        Operator fetch = new Operator(Operator.FETCH, 12000, 1);
        Operator ridScan = new Operator(Operator.RID_SCAN, 8000, 1);
        Operator join = new Operator(Operator.MERGE_SORT_JOIN, 9000, 1);
        Operator idxScan = mock(Operator.class);

        // check table scan
        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(tblScan)).thenReturn(ridScan);
        assertThat(
                extractCostOfLeafAndRemoveFetch(sqlPlan, tblScan), 
                is(tblScan.getAccumulatedCost()));

        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(tblScan)).thenReturn(fetch);
        assertThat(
                extractCostOfLeafAndRemoveFetch(sqlPlan, tblScan), 
                is(tblScan.getAccumulatedCost()));

        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(tblScan)).thenReturn(join);
        assertThat(
                extractCostOfLeafAndRemoveFetch(sqlPlan, tblScan), 
                is(tblScan.getAccumulatedCost()));

        // check index scan without fetch
        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(idxScan)).thenReturn(ridScan);
        assertThat(
                extractCostOfLeafAndRemoveFetch(sqlPlan, idxScan), 
                is(idxScan.getAccumulatedCost()));

        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(idxScan)).thenReturn(fetch);
        assertThat(
                extractCostOfLeafAndRemoveFetch(sqlPlan, idxScan), is(fetch.getAccumulatedCost()));

        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(idxScan)).thenReturn(join);
        assertThat(
                extractCostOfLeafAndRemoveFetch(sqlPlan, idxScan), 
                is(idxScan.getAccumulatedCost()));

        // check index scan with fetch, but index scan as the only child

        // check index scan with fetch, and fetch with more than one child (idx scan has siblings)
        */
    }

    // TODO:
    //  * replaceLeafBySlot
    //  * instantiatePlan
}
