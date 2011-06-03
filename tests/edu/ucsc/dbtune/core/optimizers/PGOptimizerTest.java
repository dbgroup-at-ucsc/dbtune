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

import edu.ucsc.dbtune.core.optimizers.plan.StatementPlan;
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
			"           \"Plan Rows\": 10000,                  " +
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
			"         }                                        " +
			"       ]                                          " +
			"     }                                            " +
			"   }                                              " +
			"]";

		StatementPlan plan = PGOptimizer.parseJSON(new StringReader(jsonPlan));

		assertEquals(4,           plan.size());
		assertEquals(25005000,    plan.getRootOperator().getCardinality());
		assertEquals("Hash Join", plan.getRootOperator().getName());
		assertEquals(375510.00,   plan.getRootOperator().getAccumulatedCost(), 0.01);
		assertEquals(2,           plan.getChildren(plan.getRootElement()).size());
		assertEquals(0.00,        plan.getChildren(plan.getRootElement()).get(0).getCost(), 0.01);
		assertEquals(155.00,      plan.getChildren(plan.getRootElement()).get(1).getCost(), 0.01);
		assertEquals(1,           plan.getChildren(plan.getChildren(plan.getRootElement()).get(0)).size());
		assertEquals(155.00,      plan.getChildren(plan.getChildren(plan.getRootElement()).get(1)).get(0).getCost(), 0.01);
	}

	/**
	 * Checks that the class can execute an explain statement and return a plan.
	 */
    @Test
    public void testPlanExtraction() throws Exception {
		/*
		XXX: this should be implemented using a JDBC mock

		StatementPlan plan = optimizer.explain("select * from tbl t1, tbl t2 where t1.a=t2.a;");

		assertEquals(plan.size(),4);
		*/
	}
}
