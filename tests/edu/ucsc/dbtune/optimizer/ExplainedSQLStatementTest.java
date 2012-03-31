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

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
        Set<Index> empty;
        Set<Index> updated;
        Map<Index, Double> indexCosts;
        Map<Index, Double> emptyIndexCosts;
        Index a;
        Index b;
        Index c;
        double selectCost;
        double costA;
        double baseTableCost;
        int count;

        // a SELECT
        sql = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5 AND b = 3");
        selectCost = 100.0;
        count = 1;
        cat = configureCatalog();

        conf = new HashSet<Index>(cat.findSchema("schema_0").indexes());
        used = new HashSet<Index>();
        empty = new HashSet<Index>();
        emptyIndexCosts = new HashMap<Index, Double>();
        a = cat.schemas().get(0).indexes().get(1);
        b = cat.schemas().get(0).indexes().get(3);
        c = cat.schemas().get(0).indexes().get(2);

        used.add(a);
        used.add(b);

        estmt =
            new ExplainedSQLStatement(
                sql, null, optimizer, selectCost, null, 0.0, emptyIndexCosts, conf, used, count);

        assertThat(estmt.getConfiguration(), is(conf));
        assertThat(estmt.getSelectCost(), is(selectCost));
        assertThat(estmt.getOptimizationCount(), is(count));
        assertThat(estmt.getStatement(), is(sql));
        assertThat(estmt.getTotalCost(), is(selectCost));
        assertThat(estmt.getUpdateCost(conf), is(0.0));
        assertThat(estmt.isUsed(a), is(true));
        assertThat(estmt.isUsed(b), is(true));
        assertThat(estmt.isUsed(c), is(false));
        assertThat(estmt.getUpdateCost(a), is(0.0));
        assertThat(estmt.getUpdateCost(b), is(0.0));
        assertThat(estmt.getUpdateCost(c), is(0.0));
        assertThat(estmt.getUpdateCost(), is(0.0));
        assertThat(estmt.getUsedConfiguration(), is(used));
        assertThat(estmt.getUpdatedConfiguration(), is(empty));

        // an UPDATE, with update-cost-brakedown, i.e.:
        //   update cost == (basetable + update-cost-for-each-updated-index)
        sql = new SQLStatement("UPDATE one_table.tbl SET a = 1 WHERE a = 5 AND b = 3");
        indexCosts = new HashMap<Index, Double>();
        updated = new HashSet<Index>();
        baseTableCost = 150.0;
        costA = 50.0;

        updated.add(a);
        indexCosts.put(a, costA);

        estmt =
            new ExplainedSQLStatement(
                sql, null, optimizer, selectCost, a.getTable(), baseTableCost,
                indexCosts, conf, used, count);

        assertThat(estmt.getConfiguration(), is(conf));
        assertThat(estmt.getOptimizationCount(), is(count));
        assertThat(estmt.getStatement(), is(sql));
        assertThat(estmt.getSelectCost(), is(selectCost));
        assertThat(estmt.getUpdateCost(), is(baseTableCost + costA));
        assertThat(estmt.getTotalCost(), is(selectCost + baseTableCost + costA));
        assertThat(estmt.getUpdateCost(conf), is(costA));
        assertThat(estmt.getUpdateCost(a), is(costA));
        assertThat(estmt.getUpdateCost(b), is(0.0));
        assertThat(estmt.getUpdateCost(c), is(0.0));
        assertThat(estmt.getUpdateCost(), is(greaterThanOrEqualTo(costA)));
        assertThat(estmt.getUsedConfiguration(), is(used));
        assertThat(estmt.getUpdatedConfiguration(), is(updated));

        // equals
        ExplainedSQLStatement estmt2 =
            new ExplainedSQLStatement(
                sql, null, optimizer, selectCost, a.getTable(), baseTableCost,
                indexCosts, conf, used, count);

        assertThat(estmt, is(estmt2));
    }
}
