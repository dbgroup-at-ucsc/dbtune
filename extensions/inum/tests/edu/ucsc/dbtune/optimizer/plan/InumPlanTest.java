package edu.ucsc.dbtune.optimizer.plan;

import org.junit.Test;

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
     */
    @Test
    public void testOperatorCostExtraction()
    {
        SQLStatementPlan sqlPlan;

        Operator tblScan = new Operator(Operator.TABLE_SCAN, 3000, 1);
        Operator fetch = new Operator(Operator.FETCH, 12000, 1);
        Operator ridScan = new Operator(Operator.RID_SCAN, 8000, 1);
        Operator idxScan = new Operator(Operator.INDEX_SCAN, 5000, 1);
        Operator join = new Operator(Operator.MERGE_SORT_JOIN, 9000, 1);

        // check index scan
        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(idxScan)).thenReturn(ridScan);
        assertThat(InumPlan.extractCostOfLeaf(sqlPlan, idxScan), is(ridScan.getAccumulatedCost()));

        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(idxScan)).thenReturn(fetch);
        assertThat(InumPlan.extractCostOfLeaf(sqlPlan, idxScan), is(fetch.getAccumulatedCost()));

        sqlPlan = mock(SQLStatementPlan.class);
        when(sqlPlan.getParent(idxScan)).thenReturn(join);
        assertThat(InumPlan.extractCostOfLeaf(sqlPlan, idxScan), is(idxScan.getAccumulatedCost()));

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
}
