package edu.ucsc.dbtune.optimizer;

import java.sql.ResultSet;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.makeResultSet;
import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import edu.ucsc.dbtune.metadata.Catalog;

import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;

/**
 * @author Ivo Jimenez
 */
public class DB2OptimizerTest
{
    private ResultSet rs;
    private Catalog cat;

    /**
     * @throws Exception
     *      if the creation of the mock fails
     */
    @Before
    public void setUp() throws Exception
    {
        String[] h;
        
        h = new String[7];

        h[0] = "node_id";
        h[1] = "parent_id";
        h[2] = "operator_name";
        h[3] = "object_schema";
        h[4] = "object_name";
        h[5] = "cardinality";
        h[6] = "cost";
        
        rs = makeResultSet(
                Arrays.asList(h[0], h[1], h[2],     h[3], h[4],              h[5], h[6]  ),
                Arrays.asList(1, 0, "RETURN", "schema_0", null             , 10l , 2000.0),
                Arrays.asList(2, 1, "TBSCAN", "schema_0", "table_0"        , 10l , 2000.0),
                Arrays.asList(3, 2, "SORT"  , "schema_0", null             , 10l , 1500.0),
                Arrays.asList(4, 3, "FETCH" , "schema_0", null             , 10l , 1400.0),
                Arrays.asList(5, 4, "IXSCAN", "schema_0", "table_0_index_0", 100l,  700.0));

        cat = configureCatalog();
    }

    /**
     * Checks that the extraction of {@link Operator} objects is done correctly.
     *
     * @throws Exception
     *      if the creation of the mock fails
     */
    @Test
    public void testNodeParsing() throws Exception
    {
        Operator op;

        rs.next();

        op = DB2Optimizer.parseNode(cat, rs);

        assertThat(op.getId(), is(0));
        assertThat(op.getName(), is("RETURN"));
        assertThat(op.getCardinality(), is(10l));
        assertThat(op.getAccumulatedCost(), is(2000.0));
        assertThat(op.getDatabaseObjects().isEmpty(), is(true));
        assertThat(op.getCost(), is(0.0));

        rs.next();

        op = DB2Optimizer.parseNode(cat, rs);

        assertThat(op.getId(), is(0));
        assertThat(op.getName(), is("TBSCAN"));
        assertThat(op.getCardinality(), is(10l));
        assertThat(op.getAccumulatedCost(), is(2000.0));
        assertThat(op.getDatabaseObjects().isEmpty(), is(false));
        assertThat(op.getDatabaseObjects().size(), is(1));
        assertThat(op.getDatabaseObjects().get(0).getName(), is("table_0"));
        assertThat(op.getCost(), is(0.0));

        rs.next();
        rs.next();
        rs.next();

        op = DB2Optimizer.parseNode(cat, rs);

        assertThat(op.getId(), is(0));
        assertThat(op.getName(), is("IXSCAN"));
        assertThat(op.getCardinality(), is(100l));
        assertThat(op.getAccumulatedCost(), is(700.0));
        assertThat(op.getDatabaseObjects().isEmpty(), is(false));
        assertThat(op.getDatabaseObjects().size(), is(1));
        assertThat(op.getDatabaseObjects().get(0).getName(), is("table_0_index_0"));
        assertThat(op.getCost(), is(0.0));

        assertThat(rs.next(), is(false));
    }

    /**
     * Checks that the extraction of a plan is done correctly.
     *
     * @throws Exception
     *      if the creation of the mock fails
     */
    @Test
    public void testPlanParsing() throws Exception
    {
        SQLStatementPlan plan = DB2Optimizer.parsePlan(cat, rs);

        assertThat(plan.size(), is(5));
        assertThat(plan.getRootOperator().getId(), is(1));
        assertThat(plan.getIndexes().size(), is(1));
        assertThat(plan.getRootOperator().getName(), is("RETURN"));
    }
}
