package edu.ucsc.dbtune.optimizer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit test for ExplainedSQLStatementTest.
 *
 * @author Ivo Jimenez
 */
public class ExplainedSQLStatementTest
{
    private static Optimizer optimizer;

    /**
     * Setup for the test.
     */
    @BeforeClass
    public static void beforeClassSetUp()
    {
        optimizer = mock(Optimizer.class);
    }

    /**
     * Setup for each test.
     */
    @Before
    public void setUp()
    {
    }

    /**
     * @throws Exception
     *      if 
     */
    @Test
    public void testGetters() throws Exception
    {
        ExplainedSQLStatement estmt;
        SQLStatement sql;
        Catalog cat;
        Set<Index> conf;
        Set<Index> used;
        Map<Index, Double> updateCost;
        Index a;
        Index b;
        Index c;
        double cost;
        double costA;
        double costB;
        int count;

        // a SELECT
        sql = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5");
        cost = 100.0;
        count = 10;
        cat = configureCatalog();

        conf = new HashSet<Index>(cat.schemas().get(0).indexes());
        used = new HashSet<Index>();
        updateCost = new HashMap<Index, Double>();
        a = cat.schemas().get(0).indexes().get(1);
        b = cat.schemas().get(0).indexes().get(3);
        c = cat.schemas().get(0).indexes().get(2);

        for (Index i : conf)
            updateCost.put(i, 0.0);
        
        used.add(a);
        used.add(b);

        estmt =
            new ExplainedSQLStatement(
                    sql, null, optimizer, cost, updateCost, conf, used, count);

        assertThat(estmt.getConfiguration(), is(conf));
        assertThat(estmt.getCost(), is(cost));
        assertThat(estmt.getOptimizationCount(), is(count));
        assertThat(estmt.getStatement(), is(sql));
        assertThat(estmt.getTotalCost(), is(cost));
        assertThat(estmt.getUpdateCost(conf), is(0.0));
        assertThat(estmt.getUpdateCost(a), is(0.0));
        assertThat(estmt.isUsed(a), is(true));
        assertThat(estmt.getUpdateCost(b), is(0.0));
        assertThat(estmt.isUsed(b), is(true));
        assertThat(estmt.getUpdateCost(), is(0.0));
        assertThat(estmt.getUpdatedConfiguration(), is((Set<Index>) new HashSet<Index>()));
        assertThat(estmt.getUsedConfiguration(), is(used));
        assertThat(estmt.isUsed(c), is(false));

        // an UPDATE
        sql = new SQLStatement("UPDATE one_table.tbl SET a = 1 WHERE a = 5");
        updateCost = new HashMap<Index, Double>();
        costA = 20.0;
        costB = 50.0;

        for (Index i : conf)
            updateCost.put(i, 0.0);
        
        updateCost.put(a, costA);
        updateCost.put(b, costB);

        estmt =
            new ExplainedSQLStatement(
                    sql, null, optimizer, cost, updateCost, conf, new HashSet<Index>(), count);

        assertThat(estmt.getConfiguration(), is(conf));
        assertThat(estmt.getCost(), is(cost));
        assertThat(estmt.getOptimizationCount(), is(count));
        assertThat(estmt.getStatement(), is(sql));
        assertThat(estmt.getTotalCost(), is(cost + costA + costB));
        assertThat(estmt.getUpdateCost(conf), is(costA + costB));
        assertThat(estmt.getUpdateCost(a), is(costA));
        assertThat(estmt.getUpdateCost(b), is(costB));
        assertThat(estmt.getUpdateCost(), is(costA + costB));
        assertThat(estmt.getUpdatedConfiguration(), is(used));
        assertThat(estmt.getUsedConfiguration(), is((Set<Index>) new HashSet<Index>()));
    }
}
