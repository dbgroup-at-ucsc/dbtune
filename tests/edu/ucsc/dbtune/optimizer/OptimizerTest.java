package edu.ucsc.dbtune.optimizer;

import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.BitArraySet;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.metadata.Index.SECONDARY;
import static edu.ucsc.dbtune.metadata.Index.UNCLUSTERED;
import static edu.ucsc.dbtune.metadata.Index.NON_UNIQUE;
import static edu.ucsc.dbtune.DBTuneInstances.configureAny;
import static org.junit.Assert.assertThat;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;


/**
 * Unit test for optimizer implementations.
 * <p>
 * Checks, among other things, that an optimizer is well-behaved and complies to the monotonicity 
 * and sanity properties. For more information on what these properties mean, refer to page 57 
 * (Chapter 4, Definition 4.3, Property 4.1 and 4.2 respectively).
 *
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 * @see <a
 * href="http://proquest.umi.com/pqdlink?did=2171968721&Fmt=7&clientId=1565&RQT=309&VName=PQD">
 *     "On-line Index Selection for Physical Database Tuning"</a>
 */
public class OptimizerTest
{
    public static Environment env;

    // XXX: test with a workload, eg. tpc-ds

    /**
     */
    @BeforeClass
    public static void setUp() throws Exception
    {
        env = new Environment(configureAny());
        // XXX: issue #104 setUp mock objects and complete all the empty test methods
    }

    /**
     * Checks that each supported optimizer returns not-null instances
     */
    @Test
    public void testNotNull() throws Exception
    {
    }

    /**
     * @see OptimizerTest#checkExplain
     */
    @Test
    public void testExplain() throws Exception
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
    public void testWhatIfExplain() throws Exception
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
    public void testRecommendIndexes() throws Exception
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
    public void testUsedConfiguration() throws Exception
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
    public void testIsWellBehaved() throws Exception
    {
        /*
        for (Optimizer opt : getSupportedOptimizersIterator(env)) {
            checkIsWellBehaved(opt);
        }
        */
    }

    /**
     * Checks that each supported optimizer complies with the monotonicity property
     *
     * @see checkMonotonicity
     */
    @Test
    public void testMonotonicity() throws Exception
    {
        /*
        for (Optimizer opt : getSupportedOptimizersIterator(env)) {
            checkMonotonicity(opt);
        }
        */
    }

    /**
     * Checks that each supported optimizer complies with the sanity property
     *
     * @see checkSanity
     */
    @Test
    public void testSanity() throws Exception
    {
        /*
        for (Optimizer opt : getSupportedOptimizersIterator(env)) {
            checkSanity(opt);
        }
        */
    }

    /**
     * Checks that a basic "regular" explain operation is done correctly. A "regular" cost 
     * estimation is an optimization call without hypothetical structures (or one empty hypothetical 
     * configuration).
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
        cost1 = sqlp.getCost();

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.SELECT), is(true));
        assertThat(sqlp.getCost(), greaterThan(0.0));
        assertThat(sqlp.getOptimizationCount(), is(1));

        conf  = new BitArraySet<Index>();
        sqlp  = opt.explain(sql, conf);
        cost2 = sqlp.getCost();
        
        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.SELECT), is(true));
        assertThat(sqlp.getCost(), greaterThan(0.0));
        assertThat(sqlp.getOptimizationCount(), is(1));

        assertThat(cost1, is(cost2));

        sql  = new SQLStatement("UPDATE one_table.tbl set a = 3 where a = 5");
        sqlp = opt.explain(sql);

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.UPDATE), is(true));
        assertThat(sqlp.getCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getUpdateCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getOptimizationCount(), is(1));

        sqlp = opt.explain(sql,conf);

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.UPDATE), is(true));
        assertThat(sqlp.getCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getUpdateCost(), is(greaterThan(0.0)));
        assertThat(sqlp.getOptimizationCount(), is(1));
    }

    /**
     * Checks that the given optimizer can execute basic what-if optimizations
     */
    protected static void checkWhatIfExplain(Catalog cat, Optimizer opt) throws Exception
    {
        SQLStatement sql;
        ExplainedSQLStatement sqlp;
        Column col;
        Set<Index> conf;
        Index idxa;
        Index idxb;
        double cost1;
        double cost2;

        sql   = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5");
        cost1 = opt.explain(sql).getCost();

        idxa = new Index(cat.<Column>findByName("one_table.tbl.a"));
        conf = new BitArraySet<Index>();

        conf.add(idxa);

        sqlp  = opt.explain(sql, conf);
        cost2 = sqlp.getCost();

        assertThat(sqlp, notNullValue());
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.SELECT), is(true));
        assertThat(sqlp.getCost(), greaterThan(0.0));
        assertThat(sqlp.getOptimizationCount(), is(1));
        assertThat(cost1, is(not(cost2)));

        col  = cat.<Column>findByName("one_table.tbl.b");
        idxb = new Index(col, SECONDARY, UNCLUSTERED, NON_UNIQUE);

        conf.add(idxb);

        sql  = new SQLStatement("UPDATE one_table.tbl set a = 3 where a = 5");
        sqlp = opt.explain(sql, conf);

        assertThat(sqlp.getCost(), is(cost2));
        assertThat(sqlp.getStatement().getSQLCategory().isSame(SQLCategory.UPDATE), is(true));
        assertThat(sqlp.getTotalCost(), is(greaterThan(sqlp.getCost())));
        assertThat(sqlp.getTotalCost(), is(greaterThan(sqlp.getUpdateCost())));
        assertThat(sqlp.getTotalCost(), is(sqlp.getCost() + sqlp.getUpdateCost()));

        assertThat(sqlp.getUpdateCost(), is(greaterThan(sqlp.getUpdateCost(idxa))));
        assertThat(sqlp.getUpdateCost(), is(greaterThan(sqlp.getUpdateCost(idxb))));
        //assertThat(sqlp.getUpdateCost(), is(sqlp.getUpdateCost(conf)));

        idxa.getSchema().remove(idxa);
        idxb.getSchema().remove(idxb);
    }

    /**
     * Checks that the given optimizer can execute basic index recommendation operations
     */
    protected static void checkRecommendIndexes(Optimizer opt) throws Exception
    {
        SQLStatement sql;
        Set<Index> rec;
        
        sql = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5");
        rec = opt.recommendIndexes(sql);

        assertThat(rec.isEmpty(), is(false));

        sql = new SQLStatement("UPDATE one_table.tbl SET a = -1 WHERE a = 5");
        rec = opt.recommendIndexes(sql);

        assertThat(rec.isEmpty(), is(false));
    }

    /**
     * Checks that for prepared statements generated by the given optimizer, the corresponding set 
     * of used physical structures is correct.
     */
    protected static void checkUsedConfiguration(Catalog cat, Optimizer opt) throws Exception
    {
        SQLStatement sql;
        ExplainedSQLStatement sqlp;
        Column col;
        Set<Index> conf;
        Index idxa;
        Index idxb;
        
        col  = cat.<Column>findByName("one_table.tbl.a");
        idxa = new Index(col,SECONDARY,UNCLUSTERED,NON_UNIQUE);
        conf = new BitArraySet<Index>();
        col  = cat.<Column>findByName("one_table.tbl.b");
        idxb = new Index(col,SECONDARY,UNCLUSTERED,NON_UNIQUE);

        sql  = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5");
        sqlp = opt.explain(sql, conf);

        assertThat(sqlp.getUsedConfiguration().size(), is(0));
        assertThat(sqlp.getUsedConfiguration().contains(idxa), is(false));

        sql  = new SQLStatement("UPDATE one_table.tbl set a = 3 where a > 0");
        sqlp = opt.explain(sql, conf);

        assertThat(sqlp.getUpdatedConfiguration().contains(idxa), is(false));
        assertThat(sqlp.getUpdatedConfiguration().contains(idxb), is(false));

        conf.add(idxa);

        sql  = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5");
        sqlp = opt.explain(sql, conf);

        assertThat(sqlp.getUsedConfiguration().size(), is(1));
        assertThat(sqlp.getUsedConfiguration().contains(idxa), is(true));

        sql  = new SQLStatement("UPDATE one_table.tbl set a = 3 where a > 0");
        sqlp = opt.explain(sql, conf);

        assertThat(sqlp.getUsedConfiguration().contains(idxa), is(true));
        //assertThat(sqlp.getUpdatedConfiguration().contains(idxa), is(true));
        //assertThat(sqlp.getUpdatedConfiguration().contains(idxb), is(false));

        conf.add(idxb);

        sql  = new SQLStatement("SELECT a FROM one_table.tbl WHERE a = 5");
        sqlp = opt.explain(sql, conf);

        assertThat(sqlp.getUsedConfiguration().contains(idxa), is(true));
        assertThat(sqlp.getUsedConfiguration().contains(idxb), is(false));

        assertThat(conf.containsAll(sqlp.getUsedConfiguration()), is(true));
        assertThat(conf.size(), greaterThan(sqlp.getUsedConfiguration().size()));

        sql  = new SQLStatement("UPDATE one_table.tbl set a = 3 where a > 0");
        sqlp = opt.explain(sql, conf);

        assertThat(sqlp.getUsedConfiguration().contains(idxa), is(true));
        //assertThat(sqlp.getUpdatedConfiguration().contains(idxa), is(true));
        //assertThat(sqlp.getUpdatedConfiguration().contains(idxb), is(true));

        idxa.getSchema().remove(idxa);
        idxb.getSchema().remove(idxb);
    }

    /// XXX:
    // protected static void checkAnalysisTime(Optimizer opt) throws Exception

    /**
     * Checks that the optimizer is well behaved
     */
    protected static void checkIsWellBehaved(Optimizer opt) throws Exception
    {
        // this can't be done unless there's a way of getting the set of alternative plans that the 
        // optimizer is using internally after each EXPLAIN. This isn't necessary and it's left here 
        // just to keep it on the record.
    }

    /**
     * Checks that the optimizer respects the monotonicity property:
     *
     *    For any index-sets X, Y and query q, if X ⊆ Y then cost(q, X) ≥ cost(q, Y )
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
        idx  = new Index(col,SECONDARY,UNCLUSTERED,NON_UNIQUE);
        conf = new BitArraySet<Index>();

        conf.add(idx);

        cost1 = opt.explain(sql).getCost();
        cost2 = opt.explain(sql, conf).getCost();

        assertThat(cost1, greaterThanOrEqualTo(cost2));

        idx.getSchema().remove(idx);
    }

    /**
     * Checks that the given optimizer complies with the sanity property:
     *    
     * For any index-sets X, Y and query q:
     *
     *   if used (q, Y) ⊆ X ⊆ Y, then optplan(q, X) = optplan(q, Y)
     */
    protected static void checkSanity(Catalog cat, Optimizer opt) throws Exception
    {
        SQLStatement sql;
        Column colA;
        Column colB;
        Set<Index> conf;
        Index idxA;
        Index idxB;
        double cost1;
        double cost2;

        sql  = new SQLStatement("select a from one_table.tbl where a = 5");
        colA = cat.<Column>findByName("one_table.tbl.a");
        colB = cat.<Column>findByName("one_table.tbl.b");
        idxA = new Index(colA);
        idxB = new Index(colB);
        conf = new BitArraySet<Index>();

        conf.add(idxA);

        cost1 = opt.explain(sql, conf).getCost();

        conf.add(idxB);

        cost2 = opt.explain(sql, conf).getCost();

        assertThat(cost1, is(cost2));

        // XXX: we should also check:
        //   * the set of used indexes of each plan is the same
        //   * the contents of the plan (the tree) are the same
        idxA.getSchema().remove(idxA);
        idxB.getSchema().remove(idxB);
    }
}
