/* ************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.core.optimizers;

import edu.ucsc.dbtune.core.metadata.Column;
import edu.ucsc.dbtune.core.metadata.Configuration;
import edu.ucsc.dbtune.core.metadata.Index;
import edu.ucsc.dbtune.core.metadata.Table;
import edu.ucsc.dbtune.core.metadata.Schema;
import edu.ucsc.dbtune.core.metadata.SQLTypes;
import edu.ucsc.dbtune.core.optimizers.plan.Operator;
import edu.ucsc.dbtune.core.optimizers.plan.SQLStatementPlan;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.util.SQLScriptExecuter;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.sql.Connection;

import static edu.ucsc.dbtune.core.JdbcConnectionManager.makeDatabaseConnectionManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Depends on the 'one_table' workload. Assumes that we're connecting to a PostgreSQL 9.0 system or 
 * greater.
 *
 * @author Ivo Jimenez (ivo@cs.ucsc.edu.com)
 */
public class PGOptimizerTest {
    private static Environment environment;
    private static Connection  connection;
	private static PGOptimizer optimizer;

	/**
	 * Executes the SQL script that should contain the 'one_table' workload.
     */
    @BeforeClass
    public static void setUp() throws Exception
    {
		/*
		XXX: this should be implemented using a JDBC mock

		String ddlfilename;

		environment = Environment.getInstance();
		connection  = makeDatabaseConnectionManager(environment.getAll()).connect().getJdbcConnection();
		ddlfilename = environment.getScriptAtWorkloadsFolder("one_table/create.sql");

		SQLScriptExecuter.execute(connection, ddlfilename);
		*/
    }

	/**
	 * Checks that the code is verifying the PostgreSQL version correctly.
	 */
    @Test
    public void testConstructor() throws Exception {
		/*
		XXX: this should be implemented using a JDBC mock
        try {
			optimizer = new PGOptimizer(connection);
        } catch(UnsupportedOperationException ex) {
            fail("Getting exception when we shouldn't. Are we connecting to postgres-9.0 or greater?");
		}
		*/
    }

	/**
	 * Checks that JSON-to-plan conversion is done correctly.
	 */
    @Test
    public void testJSONToPlanConversion() throws Exception {
		String jsonPlan =
			"[                                                 " +
			"   {                                              " +
			"     \"Plan\": {                                  " +
			"       \"Node Type\": \"Hash Join\",              " +
			"       \"Join Type\": \"Inner\",                  " +
			"       \"Startup Cost\": 280.00,                  " +
			"       \"Total Cost\": 375510.00,                 " +
			"       \"Plan Rows\": 25005000,                   " +
			"       \"Plan Width\": 32,                        " +
			"       \"Hash Cond\": \"(t1.a = t2.a)\",          " +
			"       \"Plans\": [                               " +
			"         {                                        " +
			"           \"Node Type\": \"Seq Scan\",           " +
			"           \"Parent Relationship\": \"Outer\",    " +
			"           \"Relation Name\": \"tbl\",            " +
			"           \"Alias\": \"t1\",                     " +
			"           \"Startup Cost\": 0.00,                " +
			"           \"Total Cost\": 155.00,                " +
			"           \"Plan Rows\": 10000,                  " +
			"           \"Plan Width\": 16                     " +
			"         },                                       " +
			"         {                                        " +
			"           \"Node Type\": \"Hash\",               " +
			"           \"Parent Relationship\": \"Inner\",    " +
			"           \"Startup Cost\": 155.00,              " +
			"           \"Total Cost\": 155.00,                " +
			"           \"Plan Rows\": 184,                    " +
			"           \"Plan Width\": 16,                    " +
			"           \"Plans\": [                           " +
			"             {                                    " +
			"               \"Node Type\": \"Seq Scan\",       " +
			"               \"Parent Relationship\": \"Outer\"," +
			"               \"Relation Name\": \"tbl\",        " +
			"               \"Alias\": \"t2\",                 " +
			"               \"Startup Cost\": 0.00,            " +
			"               \"Total Cost\": 155.00,            " +
			"			    \"Plan Rows\": 10000,              " +
			"               \"Plan Width\": 16                 " +
			"             }                                    " +
			"           ]                                      " +
			"         },                                       " +
			"         {                                        " +
			"           \"Node Type\": \"Index Scan\",         " +
			"           \"Startup Cost\": 0.00,                " +
			"           \"Total Cost\": 1778.00,               " +
			"           \"Plan Rows\": 28437,                  " +
			"           \"Plan Width\": 180                    " +
			"         }                                        " +
			"       ]                                          " +
			"     }                                            " +
			"   }                                              " +
			"]";

		SQLStatementPlan plan = PGOptimizer.parseJSON(new StringReader(jsonPlan), null);
		Operator         root = plan.getRootOperator();

		//System.out.println(plan);

		assertEquals(5, plan.size());

        // check root
		assertEquals("Hash Join", root.getName());
		assertEquals(25005000,    root.getCardinality());
		assertEquals(375510.00,   root.getAccumulatedCost(), 0.01);
		assertEquals(3,           plan.getChildren(root).size());

        // check first child
        Operator child1 = plan.getChildren(root).get(0);
		assertEquals("Seq Scan", child1.getName());
		assertEquals(10000,      child1.getCardinality());
		assertEquals(155.00,     child1.getAccumulatedCost(), 0.0);
		assertEquals(155.00,     child1.getCost(), 0.01);
        assertEquals(0,          plan.getChildren(child1).size());
        
        // check second child
        Operator child2 = plan.getChildren(root).get(1);
		assertEquals("Hash", child2.getName());
		assertEquals(184,    child2.getCardinality());
		assertEquals(155.00, child2.getAccumulatedCost(), 0.0);
        assertEquals(0.0,    child2.getCost(), 0.01);
		assertEquals(1,      plan.getChildren(child2).size());
        
        // check child of second child
        Operator child3 = plan.getChildren(child2).get(0);
		assertEquals("Seq Scan", child3.getName());
		assertEquals(10000,      child3.getCardinality());
		assertEquals(155.00,     child3.getAccumulatedCost(), 0.0);
		assertEquals(155.00,     child3.getCost(), 0.01);
		assertEquals(0,          plan.getChildren(child3).size());
        
        // check third child
        Operator child4 = plan.getChildren(root).get(2);
		assertEquals("Index Scan", child4.getName());
		assertEquals(28437,        child4.getCardinality());
		assertEquals(1778.00,      child4.getAccumulatedCost(), 0.0);
        assertEquals(1778.00,      child4.getCost(), 0.01);
		assertEquals(0,            plan.getChildren(child4).size());
        
	}

	/**
	 * Checks bound conversion. {@code DatabaseObject} instances should be bound to the 
	 * corresponding operators
	 */
    @Test
    public void testBoundConversion() throws Exception {
		String jsonPlan =
			"[                                                 " +
			"   {                                              " +
			"     \"Plan\": {                                  " +
			"       \"Node Type\": \"Nested Loop\",            " +
			"       \"Total Cost\": 375510.00,                 " +
			"       \"Plan Rows\": 25005000,                   " +
			"       \"Plans\": [                               " +
			"         {                                        " +
			"           \"Node Type\": \"Seq Scan\",           " +
			"           \"Relation Name\": \"tbl\",            " +
			"           \"Alias\": \"t1\",                     " +
			"           \"Total Cost\": 155.00,                " +
			"           \"Plan Rows\": 10000                   " +
			"         },                                       " +
			"         {                                        " +
			"           \"Node Type\": \"Index Scan\",         " +
			"           \"Index Name\": \"index_a\",           " +
			"           \"Total Cost\": 1778.00,               " +
			"           \"Plan Rows\": 28437                   " +
			"         }                                        " +
			"       ]                                          " +
			"     }                                            " +
			"   }                                              " +
			"]";

		Schema sch = new Schema("test");
		Table  tbl = new Table("tbl");
		Column col = new Column("a", SQLTypes.INTEGER);
		Index  idx = new Index("index_a",tbl,false,false,false);

		tbl.add(col);
		idx.add(col);
		tbl.add(idx);
		sch.add(tbl);
		sch.setBaseConfiguration(new Configuration(tbl.getIndexes()));

		SQLStatementPlan plan = PGOptimizer.parseJSON(new StringReader(jsonPlan), sch);
		Operator         root = plan.getRootOperator();

		//System.out.println(plan);

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

	/**
	 * Checks that the class can execute an explain statement and return a plan.
	 */
    @Test
    public void testPlanExtraction() throws Exception {
		/*
		XXX: this should be implemented using a JDBC mock

		SQLStatementPlan plan = optimizer.explain("select * from tbl t1, tbl t2 where t1.a=t2.a;");

		assertEquals(plan.size(),4);
		*/
	}
}
