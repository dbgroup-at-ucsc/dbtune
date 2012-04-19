package edu.ucsc.dbtune.optimizer;

import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
//import edu.ucsc.dbtune.util.BitArraySet;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.metadata.Index.DESCENDING;
import static edu.ucsc.dbtune.optimizer.plan.Operator.DELETE;
import static edu.ucsc.dbtune.optimizer.plan.Operator.INSERT;
import static edu.ucsc.dbtune.optimizer.plan.Operator.TABLE_SCAN;
import static edu.ucsc.dbtune.optimizer.plan.Operator.UPDATE;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Unit test for optimizer implementations.
 * <p>
 * Checks, among other things, that an optimizer is well-behaved and complies to the monotonicity 
 * and sanity properties. For more information on what these properties mean, refer to page 57 
 * (Chapter 4, Definition 4.3, Property 4.1 and 4.2 respectively).
 *
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 * @see <a href="http://http://bit.ly/wXaQC3">
 *         "On-line Index Selection for Physical Database Tuning"
 *      </a>
 */
public class OptimizerTest
{
    /**
     */
    @BeforeClass
    public static void beforeClass()
    {
        // env = new Environment(configureAny());
        // note: issue #104 setUp mock objects and complete all the empty test methods
    }

    /**
     * Checks that each supported optimizer returns not-null instances.
     */
    @Test
    public void testNotNull()
    {
    }

    /**
     * @see OptimizerTest#checkExplain
     */
    @Test
    public void testExplain()
    {
        /*
        for (Optimizer opt : getSupportedOptimizersIterator(env)) {
            checkUsedExplain(opt);
        }
        */
    }

    /**
     * @see OptimizerTest#checkWhatIfExplain
     */
    @Test
    public void testWhatIfExplain()
    {
        /*
        for (Optimizer opt : getSupportedOptimizersIterator(env)) {
            checkWhatIfExplain(opt);
        }
        */
    }

    /**
     * @see OptimizerTest#checkRecommendIndexes
     */
    @Test
    public void testRecommendIndexes()
    {
        /*
        for (Optimizer opt : getSupportedOptimizersIterator(env)) {
            checkRecommendIndexes(opt);
        }
        */
    }

    /**
     * Checks that, prepared statements that each supported optimizer generates, the set of used 
     * physical structures is correct.
     *
     * @see checkUsedConfiguration
     */
    @Test
    public void testUsedConfiguration()
    {
        /*
        for (Optimizer opt : getSupportedOptimizersIterator(env)) {
            checkUsedConfiguration(opt);
        }
        */
    }

    /**
     * Checks that each supported optimizer is well behaved.
     *
     * @see checkIsWellBehaved
     */
    @Test
    public void testIsWellBehaved()
    {
        /*
        for (Optimizer opt : getSupportedOptimizersIterator(env)) {
            checkIsWellBehaved(opt);
        }
        */
    }

    /**
     * Checks that each supported optimizer complies with the monotonicity property.
     *
     * @see checkMonotonicity
     */
    @Test
    public void testMonotonicity()
    {
        /*
        for (Optimizer opt : getSupportedOptimizersIterator(env)) {
            checkMonotonicity(opt);
        }
        */
    }

    /**
     * Checks that each supported optimizer complies with the sanity property.
     *
     * @see checkSanity
     */
    @Test
    public void testSanity()
    {
        /*
        for (Optimizer opt : getSupportedOptimizersIterator(env)) {
            checkSanity(opt);
        }
        */
    }

    /**
     * Checks that a "regular" explain operation is done correctly. A "regular" cost estimation is 
     * an optimization call without hypothetical structures (or one empty hypothetical 
     * configuration).
     *
     * @param opt
     *      optimizer under test
     * @throws Exception
     *      if something wrong occurs
     */
    protected static void checkExplain(Optimizer opt) throws Exception
    {
        SQLStatement sql;
        ExplainedSQLStatement sqlp;
        Set<Index> conf;
        double cost1;
        double cost2;

        sql   = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5");
        sqlp  = opt.explain(sql);
        cost1 = sqlp.getSelectCost();

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.SELECT), is(true));
        assertThat(sqlp.getSelectCost(), greaterThan(0.0));
        assertThat(sqlp.getUpdateCost(), is(0.0));
        assertThat(sqlp.getBaseTableUpdateCost(), is(0.0));
        assertThat(sqlp.getSelectCost(), is(sqlp.getTotalCost()));
        assertThat(sqlp.getConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUsedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUpdatedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getOptimizationCount(), is(1));
        assertThat(sqlp.getPlan().contains(TABLE_SCAN), is(true));

        conf  = new HashSet<Index>();
        sqlp  = opt.explain(sql, conf);
        cost2 = sqlp.getSelectCost();
        
        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.SELECT), is(true));
        assertThat(sqlp.getSelectCost(), greaterThan(0.0));
        assertThat(sqlp.getUpdateCost(), is(0.0));
        assertThat(sqlp.getBaseTableUpdateCost(), is(0.0));
        assertThat(sqlp.getTotalCost(), is(sqlp.getSelectCost()));
        assertThat(sqlp.getConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUsedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUpdatedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getOptimizationCount(), is(1));
        assertThat(sqlp.getPlan().contains(TABLE_SCAN), is(true));

        assertThat(cost1, is(cost2));

        // XXX: issue #106, #144 is causing this to fail for these {
        if (!(opt instanceof MySQLOptimizer) &&
                !(opt instanceof IBGOptimizer) )
        {
        sql  = new SQLStatement("UPDATE one_table.tbl SET a = 3 WHERE a = 5");
        sqlp = opt.explain(sql);

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.UPDATE), is(true));
        assertThat(sqlp.getSelectCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getPlan().contains(UPDATE), is(true));

        // XXX: issue #139 is causing this to fail for PGOptimizer {
        if (!(opt instanceof PGOptimizer)) {
        assertThat(sqlp.getUpdateCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getBaseTableUpdateCost(), is(greaterThan(0.0)));
        }
        // }

        assertThat(sqlp.getBaseTableUpdateCost(), is(sqlp.getUpdateCost()));
        assertThat(sqlp.getTotalCost(), is(sqlp.getSelectCost() + sqlp.getUpdateCost()));
        assertThat(sqlp.getConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUsedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUpdatedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getOptimizationCount(), is(1));

        sqlp = opt.explain(sql, conf);

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.UPDATE), is(true));
        assertThat(sqlp.getSelectCost(), is(greaterThan(0.0)));

        // XXX: issue #139 is causing this to fail for PGOptimizer {
        if (!(opt instanceof PGOptimizer)) {
        assertThat(sqlp.getUpdateCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getBaseTableUpdateCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getPlan().contains(UPDATE), is(true));
        }
        // }

        assertThat(sqlp.getBaseTableUpdateCost(), is(sqlp.getUpdateCost()));

        assertThat(sqlp.getTotalCost(), is(sqlp.getSelectCost() + sqlp.getUpdateCost()));
        assertThat(sqlp.getConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUsedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUpdatedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getOptimizationCount(), is(1));

        sql  = new SQLStatement("INSERT INTO one_table.tbl VALUES(1,2,3,4)");
        sqlp = opt.explain(sql);

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.INSERT), is(true));
        assertThat(sqlp.getSelectCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getPlan().contains(INSERT), is(true));

        // XXX: issue #139 is causing this to fail for PGOptimizer {
        if (!(opt instanceof PGOptimizer)) {
        assertThat(sqlp.getUpdateCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getBaseTableUpdateCost(), is(greaterThan(0.0)));
        }
        // }

        assertThat(sqlp.getBaseTableUpdateCost(), is(sqlp.getUpdateCost()));
        assertThat(sqlp.getTotalCost(), is(sqlp.getSelectCost() + sqlp.getUpdateCost()));
        assertThat(sqlp.getConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUsedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUpdatedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getOptimizationCount(), is(1));

        sqlp = opt.explain(sql, conf);

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.INSERT), is(true));
        assertThat(sqlp.getSelectCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getPlan().contains(INSERT), is(true));

        // XXX: issue #139 is causing this to fail for PGOptimizer {
        if (!(opt instanceof PGOptimizer)) {
        assertThat(sqlp.getUpdateCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getBaseTableUpdateCost(), is(greaterThan(0.0)));
        }
        // }

        assertThat(sqlp.getBaseTableUpdateCost(), is(sqlp.getUpdateCost()));

        assertThat(sqlp.getTotalCost(), is(sqlp.getSelectCost() + sqlp.getUpdateCost()));
        assertThat(sqlp.getConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUsedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUpdatedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getOptimizationCount(), is(1));

        sql  = new SQLStatement("DELETE FROM one_table.tbl WHERE a = 5");
        sqlp = opt.explain(sql);

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.DELETE), is(true));
        assertThat(sqlp.getSelectCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getPlan().contains(DELETE), is(true));

        // XXX: issue #139 is causing this to fail for PGOptimizer {
        if (!(opt instanceof PGOptimizer)) {
        assertThat(sqlp.getUpdateCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getBaseTableUpdateCost(), is(greaterThan(0.0)));
        }
        // }

        assertThat(sqlp.getBaseTableUpdateCost(), is(sqlp.getUpdateCost()));
        assertThat(sqlp.getTotalCost(), is(sqlp.getSelectCost() + sqlp.getUpdateCost()));
        assertThat(sqlp.getConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUsedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUpdatedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getOptimizationCount(), is(1));

        sqlp = opt.explain(sql, conf);

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.DELETE), is(true));
        assertThat(sqlp.getSelectCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getPlan().contains(DELETE), is(true));

        // XXX: issue #139 is causing this to fail for PGOptimizer {
        if (!(opt instanceof PGOptimizer)) {
        assertThat(sqlp.getUpdateCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getBaseTableUpdateCost(), is(greaterThan(0.0)));
        }
        // }

        assertThat(sqlp.getBaseTableUpdateCost(), is(sqlp.getUpdateCost()));

        assertThat(sqlp.getTotalCost(), is(sqlp.getSelectCost() + sqlp.getUpdateCost()));
        assertThat(sqlp.getConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUsedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getUpdatedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getOptimizationCount(), is(1));
        }
        // }
    }

    /**
     * Checks that the given optimizer can execute basic what-if optimizations.
     *
     * @param cat
     *      catalog used to retrieve metadata
     * @param opt
     *      optimizer under test
     * @throws Exception
     *      if something wrong occurs
     */
    protected static void checkWhatIfExplain(Catalog cat, Optimizer opt) throws Exception
    {
        SQLStatement sql;
        ExplainedSQLStatement sqlp;
        Set<Index> conf;
        Index idxa;
        Index idxb;
        double cost1;
        double cost2;

        sql = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5");
        cost1 = opt.explain(sql).getSelectCost();
        idxa = new Index(cat.<Column>findByName("one_table.tbl.a"), DESCENDING);
        idxb = new Index(cat.<Column>findByName("one_table.tbl.b"), DESCENDING);
        conf = new HashSet<Index>();

        conf.add(idxb);
        conf.add(idxa);

        sqlp  = opt.explain(sql, conf);
        cost2 = sqlp.getSelectCost();

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.SELECT), is(true));
        assertThat(sqlp.getSelectCost(), greaterThan(0.0));
        assertThat(cost1, is(greaterThan(sqlp.getSelectCost())));
        assertThat(sqlp.getUpdateCost(), is(0.0));
        assertThat(sqlp.getBaseTableUpdateCost(), is(0.0));
        assertThat(sqlp.getTotalCost(), is(sqlp.getSelectCost()));
        assertThat(sqlp.getConfiguration(), is(conf));
        assertThat(sqlp.getUsedConfiguration().isEmpty(), is(false));
        assertThat(sqlp.getUsedConfiguration().size(), is(1));
        assertThat(sqlp.getUsedConfiguration().contains(idxa), is(true));
        assertThat(sqlp.getUpdatedConfiguration().isEmpty(), is(true));
        assertThat(sqlp.getOptimizationCount(), is(1));

        // XXX: issue #106, #144 is causing this to fail for these {
        if (!(opt instanceof MySQLOptimizer) &&
                !(opt instanceof IBGOptimizer))
        {
        sql  = new SQLStatement("UPDATE one_table.tbl set a = 3 where a = 5");
        sqlp = opt.explain(sql, conf);

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.UPDATE), is(true));
        assertThat(sqlp.getSelectCost(), greaterThan(0.0));
        assertThat(sqlp.getSelectCost(), is(cost2));
        assertThat(sqlp.getUpdateCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getUpdateCost(), is(greaterThan(sqlp.getUpdateCost(idxa))));
        assertThat(sqlp.getUpdateCost(), is(greaterThan(sqlp.getUpdateCost(idxb))));
        assertThat(sqlp.getUpdateCost(idxa), is(greaterThan(0.0)));
        assertThat(sqlp.getUpdateCost(idxb), is(greaterThan(0.0)));
        assertThat(sqlp.getUpdatedConfiguration(), is(conf));

        // XXX: issue #139 is causing this to fail for PGOptimizer {
        if (!(opt instanceof PGOptimizer)) {
        assertThat(sqlp.getBaseTableUpdateCost(), is(greaterThan(0.0)));
        }
        // }

        assertThat(
            sqlp.getUpdateCost(),
            is(sqlp.getBaseTableUpdateCost() + sqlp.getUpdateCost(sqlp.getUpdatedConfiguration())));
        assertThat(sqlp.getTotalCost(), is(greaterThan(sqlp.getSelectCost())));
        assertThat(sqlp.getTotalCost(), is(greaterThan(sqlp.getUpdateCost())));
        assertThat(sqlp.getTotalCost(), is(sqlp.getSelectCost() + sqlp.getUpdateCost()));
        assertThat(sqlp.getConfiguration(), is(conf));
        assertThat(sqlp.getUsedConfiguration().isEmpty(), is(false));
        assertThat(sqlp.getUsedConfiguration().size(), is(1));
        assertThat(sqlp.getUsedConfiguration().contains(idxa), is(true));
        assertThat(sqlp.getUpdatedTable(), is(idxa.getTable()));
        assertThat(sqlp.getUpdatedConfiguration().isEmpty(), is(false));
        assertThat(sqlp.getUpdatedConfiguration(), is(conf));
        assertThat(sqlp.getOptimizationCount(), is(1));

        sql  = new SQLStatement("INSERT INTO one_table.tbl VALUES(1,2,3,4)");
        sqlp = opt.explain(sql, conf);

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.INSERT), is(true));
        assertThat(sqlp.getSelectCost(), greaterThan(0.0));
        assertThat(sqlp.getUpdateCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getUpdateCost(), is(greaterThan(sqlp.getUpdateCost(idxa))));
        assertThat(sqlp.getUpdateCost(), is(greaterThan(sqlp.getUpdateCost(idxb))));
        assertThat(sqlp.getUpdateCost(idxa), is(greaterThan(0.0)));
        assertThat(sqlp.getUpdateCost(idxb), is(greaterThan(0.0)));
        assertThat(sqlp.getUpdatedConfiguration(), is(conf));

        // XXX: issue #139 is causing this to fail for PGOptimizer {
        if (!(opt instanceof PGOptimizer)) {
        assertThat(sqlp.getBaseTableUpdateCost(), is(greaterThan(0.0)));
        }
        // }

        assertThat(
            sqlp.getUpdateCost(),
            is(sqlp.getBaseTableUpdateCost() + sqlp.getUpdateCost(sqlp.getUpdatedConfiguration())));
        assertThat(sqlp.getTotalCost(), is(greaterThan(sqlp.getSelectCost())));
        assertThat(sqlp.getTotalCost(), is(greaterThan(sqlp.getUpdateCost())));
        assertThat(sqlp.getTotalCost(), is(sqlp.getSelectCost() + sqlp.getUpdateCost()));
        assertThat(sqlp.getConfiguration(), is(conf));
        assertThat(sqlp.getUpdatedTable(), is(idxa.getTable()));
        assertThat(sqlp.getUpdatedConfiguration().isEmpty(), is(false));
        assertThat(sqlp.getUpdatedConfiguration(), is(conf));
        assertThat(sqlp.getOptimizationCount(), is(1));

        sql  = new SQLStatement("DELETE FROM one_table.tbl WHERE a = 5");
        sqlp = opt.explain(sql, conf);

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.DELETE), is(true));
        assertThat(sqlp.getSelectCost(), greaterThan(0.0));
        assertThat(sqlp.getUpdateCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getUpdateCost(), is(greaterThan(sqlp.getUpdateCost(idxa))));
        assertThat(sqlp.getUpdateCost(), is(greaterThan(sqlp.getUpdateCost(idxb))));
        assertThat(sqlp.getUpdateCost(idxa), is(greaterThan(0.0)));
        assertThat(sqlp.getUpdateCost(idxb), is(greaterThan(0.0)));
        assertThat(sqlp.getUpdatedConfiguration(), is(conf));

        // XXX: issue #139 is causing this to fail for PGOptimizer {
        if (!(opt instanceof PGOptimizer)) {
        assertThat(sqlp.getBaseTableUpdateCost(), is(greaterThan(0.0)));
        }
        // }

        assertThat(
            sqlp.getUpdateCost(),
            is(sqlp.getBaseTableUpdateCost() + sqlp.getUpdateCost(sqlp.getUpdatedConfiguration())));
        assertThat(sqlp.getTotalCost(), is(greaterThan(sqlp.getSelectCost())));
        assertThat(sqlp.getTotalCost(), is(greaterThan(sqlp.getUpdateCost())));
        assertThat(sqlp.getTotalCost(), is(sqlp.getSelectCost() + sqlp.getUpdateCost()));
        assertThat(sqlp.getConfiguration(), is(conf));
        assertThat(sqlp.getUsedConfiguration().isEmpty(), is(false));
        assertThat(sqlp.getUsedConfiguration().size(), is(1));
        assertThat(sqlp.getUsedConfiguration().contains(idxa), is(true));
        assertThat(sqlp.getUpdatedTable(), is(idxa.getTable()));
        assertThat(sqlp.getUpdatedConfiguration().isEmpty(), is(false));
        assertThat(sqlp.getUpdatedConfiguration(), is(conf));
        assertThat(sqlp.getOptimizationCount(), is(1));
        }
        // }

        idxa.getSchema().remove(idxa);
        idxb.getSchema().remove(idxb);
    }

    /**
     * Checks that the given optimizer can execute basic index recommendation operations.
     *
     * @param opt
     *      optimizer under test
     * @throws Exception
     *      if something wrong occurs
     */
    protected static void checkRecommendIndexes(Optimizer opt) throws Exception
    {
        SQLStatement sql;
        Set<Index> rec;
        
        sql = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5");
        rec = opt.recommendIndexes(sql);

        assertThat(rec.isEmpty(), is(false));

        // XXX: issues #240, #241 is causing this to fail for all except DB2Optimizer {
        if (getBaseOptimizer(opt) instanceof DB2Optimizer) {
        for (Index i : rec) {
            assertThat(i.getCreationCost(), is(greaterThan(0.0)));
            assertThat(i.getBytes(), is(greaterThan(0l)));
        }
        }

        // XXX: issue #106, #144 is causing this to fail for these {
        if (!(opt instanceof MySQLOptimizer) &&
                !(opt instanceof IBGOptimizer))
        {
        sql = new SQLStatement("UPDATE one_table.tbl SET a = -1 WHERE a = 5");
        rec = opt.recommendIndexes(sql);

        assertThat(rec.isEmpty(), is(false));
        }
        // }
    }

    // protected static void checkAnalysisTime(Optimizer opt) throws Exception

    /**
     * Checks that the optimizer is well behaved.
     *
     * @param opt
     *      optimizer under test
     * @throws Exception
     *      if something wrong occurs
     */
    protected static void checkIsWellBehaved(Optimizer opt) throws Exception
    {
        // this can't be done unless there's a way of getting the set of alternative plans that the 
        // optimizer is using internally after each EXPLAIN. This isn't necessary and it's left here 
        // just to keep it on the record.
    }

    /**
     * Checks that the optimizer respects the monotonicity property. Defined by:
     *
     *    For any index-sets X, Y and query q, if X ⊆ Y then cost(q, X) ≥ cost(q, Y)
     *
     * @param cat
     *      catalog used to retrieve metadata
     * @param opt
     *      optimizer under test
     * @throws Exception
     *      if something wrong occurs
     */
    protected static void checkMonotonicity(Catalog cat, Optimizer opt) throws Exception
    {
        SQLStatement sql;
        Column col;
        Set<Index> conf;
        Index idx;
        double cost1;
        double cost2;

        sql  = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5");
        col  = cat.<Column>findByName("one_table.tbl.a");
        idx  = new Index(col, DESCENDING);
        conf = new HashSet<Index>();

        conf.add(idx);

        cost1 = opt.explain(sql).getSelectCost();
        cost2 = opt.explain(sql, conf).getSelectCost();

        assertThat(cost1, greaterThanOrEqualTo(cost2));

        idx.getSchema().remove(idx);
    }

    /**
     * Checks that the given optimizer complies with the sanity property.
     * <p>
     * For any index-sets X, Y and query q:
     *
     *   if used (q, Y) ⊆ X ⊆ Y, then optplan(q, X) = optplan(q, Y)
     *
     * @param cat
     *      catalog used to retrieve metadata
     * @param opt
     *      optimizer under test
     * @throws Exception
     *      if something wrong occurs
     */
    protected static void checkSanity(Catalog cat, Optimizer opt) throws Exception
    {
        SQLStatement sql;
        Column colA;
        Column colB;
        Set<Index> conf;
        ExplainedSQLStatement exp1;
        ExplainedSQLStatement exp2;
        Index idxA;
        Index idxB;

        sql  = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5");
        colA = cat.<Column>findByName("one_table.tbl.a");
        colB = cat.<Column>findByName("one_table.tbl.b");
        idxA = new Index(colA, DESCENDING);
        idxB = new Index(colB, DESCENDING);
        conf = new HashSet<Index>();

        conf.add(idxA);

        exp1 = opt.explain(sql, conf);

        conf.add(idxB);

        exp2 = opt.explain(sql, conf);

        assertThat(exp1.getSelectCost(), is(exp2.getSelectCost()));
        assertThat(exp1.getUpdateCost(), is(exp2.getUpdateCost()));
        assertThat(exp1.getUsedConfiguration(), is(exp2.getUsedConfiguration()));

        // we should also check the contents of the plan (the tree) are the same

        idxA.getSchema().remove(idxA);
        idxB.getSchema().remove(idxB);
    }

    /**
     * Checks that {@link PreparedSQLStatement} objects generated by the optimizer are correct.
     *
     * @param cat
     *      catalog used to retrieve metadata
     * @param opt
     *      optimizer under test
     * @throws Exception
     *      if something wrong occurs
     */
    protected static void checkPreparedExplain(Catalog cat, Optimizer opt) throws Exception
    {
        SQLStatement sql;
        PreparedSQLStatement stmt;
        ExplainedSQLStatement exp1;
        ExplainedSQLStatement exp2;
        Set<Index> conf;
        Column col;
        Index idxa;
        Index idxb;

        // regular explain
        sql = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5");
        stmt = opt.prepareExplain(sql);

        assertThat(stmt, is(notNullValue()));

        exp1 = opt.explain(sql);
        exp2 = stmt.explain(new HashSet<Index>());

        assertThat(exp1, is(notNullValue()));
        assertThat(exp2, is(notNullValue()));
        assertThat(exp2, is(exp1));

        // XXX: issue #106, #144 is causing this to fail for these {
        if (!(opt instanceof MySQLOptimizer) &&
                !(opt instanceof IBGOptimizer))
        {
        sql  = new SQLStatement("UPDATE one_table.tbl set a = 3 where a = 5");
        stmt = opt.prepareExplain(sql);

        assertThat(stmt, is(notNullValue()));

        exp1 = opt.explain(sql);
        exp2 = stmt.explain(new HashSet<Index>());

        assertThat(exp1, is(notNullValue()));
        assertThat(exp2, is(notNullValue()));
        assertThat(exp2, is(exp1));
        }
        // }

        // what-if call
        sql = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5");
        stmt = opt.prepareExplain(sql);

        assertThat(stmt, is(notNullValue()));

        idxa = new Index(cat.<Column>findByName("one_table.tbl.a"), DESCENDING);
        conf = new HashSet<Index>();

        conf.add(idxa);

        exp1 = opt.explain(sql, conf);
        exp2 = stmt.explain(conf);

        assertThat(exp1, is(notNullValue()));
        assertThat(exp2, is(notNullValue()));
        assertThat(exp2, is(exp1));

        col  = cat.<Column>findByName("one_table.tbl.b");
        idxb = new Index(col, DESCENDING);

        conf.add(idxb);

        sql = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5");
        stmt = opt.prepareExplain(sql);

        assertThat(stmt, is(notNullValue()));

        exp1 = opt.explain(sql, conf);
        exp2 = stmt.explain(conf);

        assertThat(exp1, is(notNullValue()));
        assertThat(exp2, is(notNullValue()));
        assertThat(exp2, is(exp1));

        conf.remove(idxa);

        exp1 = opt.explain(sql, conf);
        exp2 = stmt.explain(conf);

        assertThat(exp1, is(notNullValue()));
        assertThat(exp2, is(notNullValue()));
        assertThat(exp2, is(exp1));

        // XXX: issue #106, #144 is causing this to fail for these {
        if (!(opt instanceof MySQLOptimizer) &&
                !(opt instanceof IBGOptimizer))
        {
        sql  = new SQLStatement("UPDATE one_table.tbl set a = 3 where a = 5");
        stmt = opt.prepareExplain(sql);

        assertThat(stmt, is(notNullValue()));

        exp1 = opt.explain(sql, conf);
        exp2 = stmt.explain(conf);

        assertThat(exp1, is(notNullValue()));
        assertThat(exp2, is(notNullValue()));
        assertThat(exp2, is(exp1));
        }
        // }

        idxa.getSchema().remove(idxa);
        idxb.getSchema().remove(idxb);
    }

    /**
     * Checks that disabling FTS works as expected.
     *
     * @param cat
     *      catalog used to retrieve metadata
     * @param opt
     *      optimizer under test
     * @throws Exception
     *      if something wrong occurs
     */
    protected static void checkFTSDisabled(Catalog cat, Optimizer opt) throws Exception
    {
        // XXX: #175 #176 are causing this to fail for MySQL and Postgres {
        if (getBaseOptimizer(opt) instanceof MySQLOptimizer ||
                getBaseOptimizer(opt) instanceof PGOptimizer)
            return;

        SQLStatement sql;
        Set<Index> conf;
        Index idxa;

        sql = new SQLStatement("SELECT * FROM ONE_TABLE.TBL WHERE a > -1000000");
        idxa = new Index(cat.<Column>findByName("one_table.tbl.a"), DESCENDING);
        conf = new HashSet<Index>();

        conf.add(idxa);

        assertThat(opt.explain(sql, conf).getUsedConfiguration(), is(not(conf)));

        opt.setFTSDisabled(true);

        assertThat(opt.explain(sql, conf).getUsedConfiguration(), is(conf));

        opt.setFTSDisabled(false);

        idxa.getSchema().remove(idxa);
        // }
    }
}
