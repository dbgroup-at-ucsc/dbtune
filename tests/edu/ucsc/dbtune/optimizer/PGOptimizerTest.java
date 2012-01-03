package edu.ucsc.dbtune.optimizer;

import java.io.StringReader;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.SQLTypes;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Ivo Jimenez
 */
public class PGOptimizerTest
{
    /**
     * Checks that JSON-to-plan conversion is done correctly.
     *
     * @throws Exception
     *      if an i/o error occurs.
     */
    @Test
    public void testJSONToPlanConversion() throws Exception
    {
        String jsonPlan =
            "[                                                 " +
            "   {                                              " +
            "     \"Plan\": {                                  " +
            "       \"Node Type\": \"Hash Join\",             " +
            "       \"Join Type\": \"Inner\",                 " +
            "       \"Startup Cost\": 280.00,                 " +
            "       \"Total Cost\": 375510.00,                " +
            "       \"Plan Rows\": 25005000,                  " +
            "       \"Plan Width\": 32,                       " +
            "       \"Hash Cond\": \"(t1.a = t2.a)\",         " +
            "       \"Plans\": [                               " +
            "         {                                        " +
            "           \"Node Type\": \"Seq Scan\",          " +
            "           \"Parent Relationship\": \"Outer\",   " +
            "           \"Relation Name\": \"tbl\",           " +
            "           \"Alias\": \"t1\",                    " +
            "           \"Startup Cost\": 0.00,               " +
            "           \"Total Cost\": 155.00,               " +
            "           \"Plan Rows\": 10000,                 " +
            "           \"Plan Width\": 16                     " +
            "         },                                      " +
            "         {                                        " +
            "           \"Node Type\": \"Hash\",              " +
            "           \"Parent Relationship\": \"Inner\",   " +
            "           \"Startup Cost\": 155.00,             " +
            "           \"Total Cost\": 155.00,               " +
            "           \"Plan Rows\": 184,                   " +
            "           \"Plan Width\": 16,                   " +
            "           \"Plans\": [                           " +
            "             {                                    " +
            "               \"Node Type\": \"Seq Scan\",      " +
            "               \"Parent Relationship\": \"Outer\"," +
            "               \"Relation Name\": \"tbl\",       " +
            "               \"Alias\": \"t2\",                " +
            "               \"Startup Cost\": 0.00,           " +
            "               \"Total Cost\": 155.00,           " +
            "               \"Plan Rows\": 10000,             " +
            "               \"Plan Width\": 16                 " +
            "             }                                    " +
            "           ]                                      " +
            "         },                                      " +
            "         {                                        " +
            "           \"Node Type\": \"Index Scan\",        " +
            "           \"Startup Cost\": 0.00,               " +
            "           \"Total Cost\": 1778.00,              " +
            "           \"Plan Rows\": 28437,                 " +
            "           \"Plan Width\": 180                    " +
            "         }                                        " +
            "       ]                                          " +
            "     }                                            " +
            "   }                                              " +
            "]";

        SQLStatementPlan plan = PGOptimizer.parseJSON(new StringReader(jsonPlan), null);
        Operator         root = plan.getRootOperator();

        assertEquals(5, plan.size());

        // check root
        assertEquals("Hash Join", root.getName());
        assertEquals(25005000,   root.getCardinality());
        assertEquals(375510.00,  root.getAccumulatedCost(), 0.01);
        assertEquals(3,          plan.getChildren(root).size());

        // check first child
        Operator child1 = plan.getChildren(root).get(0);
        assertEquals("Seq Scan", child1.getName());
        assertEquals(10000,     child1.getCardinality());
        assertEquals(155.00,    child1.getAccumulatedCost(), 0.0);
        assertEquals(155.00,    child1.getCost(), 0.01);
        assertEquals(0,         plan.getChildren(child1).size());
        
        // check second child
        Operator child2 = plan.getChildren(root).get(1);
        assertEquals("Hash", child2.getName());
        assertEquals(184,   child2.getCardinality());
        assertEquals(155.00, child2.getAccumulatedCost(), 0.0);
        assertEquals(0.0,   child2.getCost(), 0.01);
        assertEquals(1,     plan.getChildren(child2).size());
        
        // check child of second child
        Operator child3 = plan.getChildren(child2).get(0);
        assertEquals("Seq Scan", child3.getName());
        assertEquals(10000,     child3.getCardinality());
        assertEquals(155.00,    child3.getAccumulatedCost(), 0.0);
        assertEquals(155.00,    child3.getCost(), 0.01);
        assertEquals(0,         plan.getChildren(child3).size());
        
        // check third child
        Operator child4 = plan.getChildren(root).get(2);
        assertEquals("Index Scan", child4.getName());
        assertEquals(28437,       child4.getCardinality());
        assertEquals(1778.00,     child4.getAccumulatedCost(), 0.0);
        assertEquals(1778.00,     child4.getCost(), 0.01);
        assertEquals(0,           plan.getChildren(child4).size());
        
    }

    /**
     * Checks bound conversion. {@code DatabaseObject} instances should be bound to the 
     * corresponding operators
     *
     * @throws Exception
     *      if an i/o error occurs.
     */
    @Test
    public void testBoundConversion() throws Exception
    {
        String jsonPlan =
            "[                                                 " +
            "   {                                              " +
            "     \"Plan\": {                                  " +
            "       \"Node Type\": \"Nested Loop\",           " +
            "       \"Total Cost\": 375510.00,                " +
            "       \"Plan Rows\": 25005000,                  " +
            "       \"Plans\": [                               " +
            "         {                                        " +
            "           \"Node Type\": \"Seq Scan\",          " +
            "           \"Relation Name\": \"tbl\",           " +
            "           \"Alias\": \"t1\",                    " +
            "           \"Total Cost\": 155.00,               " +
            "           \"Plan Rows\": 10000                   " +
            "         },                                      " +
            "         {                                        " +
            "           \"Node Type\": \"Index Scan\",        " +
            "           \"Index Name\": \"index_a\",          " +
            "           \"Total Cost\": 1778.00,              " +
            "           \"Plan Rows\": 28437                   " +
            "         }                                        " +
            "       ]                                          " +
            "     }                                            " +
            "   }                                              " +
            "]";

        Catalog cat = new Catalog("catalog");
        Schema  sch = new Schema(cat, "test");
        Table   tbl = new Table(sch, "tbl");

        new Column(tbl, "a", SQLTypes.INTEGER);
        new Index(sch, "index_a", false, false, false);

        SQLStatementPlan plan = PGOptimizer.parseJSON(new StringReader(jsonPlan), sch);
        Operator         root = plan.getRootOperator();

        assertEquals(3, plan.size());

        // check root
        assertEquals("Nested Loop", root.getName());
        assertEquals(2, plan.getChildren(root).size());

        // check first child
        Operator child1 = plan.getChildren(root).get(0);
        assertEquals("Seq Scan", child1.getName());
        assertEquals("tbl", child1.getDatabaseObjects().get(0).getName());
        
        // check second child
        Operator child2 = plan.getChildren(root).get(1);
        assertEquals("Index Scan", child2.getName());
        assertEquals("index_a", child2.getDatabaseObjects().get(0).getName());
    }
}
