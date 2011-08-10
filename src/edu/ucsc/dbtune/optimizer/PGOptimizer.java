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
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied  *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.core.optimizer;

import edu.ucsc.dbtune.core.metadata.DatabaseObject;
import edu.ucsc.dbtune.core.metadata.Schema;
import edu.ucsc.dbtune.core.optimizer.plan.Operator;
import edu.ucsc.dbtune.core.optimizer.plan.SQLStatementPlan;

import java.io.Reader;
import java.io.StringReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Map;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import static edu.ucsc.dbtune.util.Strings.compareVersion;
import static edu.ucsc.dbtune.connectivity.PGCommands.getVersion;

/**
 * The interface with the PostgreSQL optimizer.
 *
 * @author Ivo Jimenez
 */
public class PGOptimizer extends Optimizer
{
    private Connection connection;
    private String     explain;
    private Schema     schema;

    /**
     * Creates a new optimizer for PostgreSQL systems.
     *
     * @param connection
     *     JDBC connection used to communicate to a PostgreSQL system.
     * @param schema
     *     can be null. A {@code Schema} where metadata of an object referred by an operator is 
     *     stored. If not null, it is used to bind operator references to actual metadata objects.
     * @throws SQLException
     *     if an error occurs while communicating to the server.
     * @throws UnsupportedOperationException
     *     if the version of the PostgreSQL {@code connection} is communicating with isn't 9.0.0 or 
     *     above.
     */
    public PGOptimizer(Connection connection, Schema schema)
        throws SQLException, UnsupportedOperationException
    {
        String version = getVersion(connection);

        if(compareVersion("9.0.0", version) > 0) {
            throw new UnsupportedOperationException(
                "PostgreSQL version " + version + " doesn't produce formatted EXPLAIN plans");
        }

        this.schema     = schema;
        this.connection = connection;
        this.explain    = "EXPLAIN (COSTS true, FORMAT json) ";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLStatementPlan explain(String sql) throws SQLException {
        SQLStatementPlan plan = null;
        Statement        st   = connection.createStatement();
        ResultSet        rs   = st.executeQuery(explain + sql);

        while(rs.next()) {
            try {
                plan = parseJSON(new StringReader(rs.getString(1)), schema);
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }

        rs.close();
        st.close();

        return plan;
    }

    /**
     * Parses the content provided by {@code reader}. The format is assumed to be JSON as defined in 
     * the <a href="http://www.postgresql.org/docs/9.0/static/sql-explain.html">PostgreSQL 9.0</a> 
     * documentation.
     * <p>
     * A node has the following structure:
     * <code>
     * [
     *   {
     *     "Plan": {
     *       "Node Type": "Limit",
     *       "Startup Cost": 926895.36,
     *       "Total Cost": 926895.37,
     *       "Plan Rows": 1,
     *       "Plan Width": 0,
     *       "Plans": [
     *         {
     *            "Node Type": "Seq Scan",
     *            "Parent Relationship": "Outer",
     *            "Relation Name": "tbl",
     *            "Alias": "t1",
     *            "Startup Cost": 0.00,
     *            "Total Cost": 155.00,
     *            "Plan Rows": 10000,
     *            "Plan Width": 16
     *         },
     *         {
     *           ...
     *         }
     *       ]
     *     }
     *   }
     * ]
     * </code>
     * <p>
     * In JSON terminology, a plan is composed by an array, this one containing one single {@code 
     * Plan} object. The {@code Plan} element represents the root node and contains child nodes. 
     * Subsequent children of other nodes are contained in the {@code Plans} entry. Besides 
     * containing the list of children, a plan contains a set of attributes, like node type, costs 
     * and cardinality.
     *
     * @param reader
     *     object where the plan contents are retrieved from.
     * @param schema
     *     can be null. A {@code Schema} where metadata of an object referred by an operator is 
     *     stored. If not null, it is used to bind operator references to actual metadata objects.
     * @return
     *     the object representing the plan for the statement contained in the reader's source.
     * @throws IOException
     *     if an error occurs when reading from data from {@reader}
     * @throws SQLException
     *     when an error occurs during the parsing, eg. a data type conversion error occurs.
     */
    public static SQLStatementPlan parseJSON(Reader reader, Schema schema)
        throws IOException, SQLException
    {
        ObjectMapper     mapper;
        SQLStatementPlan plan;
        Operator         root;

        mapper = new ObjectMapper();

        @SuppressWarnings("unchecked")
        List<Map<String,Object>> planData = mapper.readValue(reader, List.class);

        if(planData == null) {
            return new SQLStatementPlan(new Operator());
        }

        if(planData.size() > 1) {
            throw new SQLException("More than one root node");
        }

        @SuppressWarnings("unchecked")
        Map<String,Object> rootData = (Map<String,Object>) planData.get(0).get("Plan");

        root = extractNode(rootData, schema);
        plan = new SQLStatementPlan(root);

        extractChildNodes(plan, root, rootData, schema);

        return plan;
    }

    /**
     * Recursively extracts child nodes contained in a {@code planData} object (structured Jackson's 
     * Simple Data Binding format) and inserts them into the given {@code Plan}.
     *
     * @param plan
     *     the plan being recursively populated
     * @param parent
     *     the operator whose corresponding children nodes (contained in {@code parentData}) are 
     *     being extracted from and assigned to.
     * @param parentData
     *     the map containing the data of plans
     * @param schema
     *     can be null. A {@code Schema} where metadata of an object referred by an operator is 
     *     stored. If not null, it is used to bind operator references to actual metadata objects.
     * @throws SQLException
     *     when an error occurs during the parsing, eg. a data type conversion error occurs.
     * @see <a href="http://wiki.fasterxml.com/JacksonDocumentation">Jackson Documentation</a>
     */
    private static void extractChildNodes(
            SQLStatementPlan   plan,
            Operator           parent,
            Map<String,Object> parentData,
            Schema             schema )
        throws SQLException
    {
        @SuppressWarnings("unchecked")
        List<Map<String,Object>> childrenData = (List<Map<String,Object>>) parentData.get("Plans");

        parent.setCost(parent.getAccumulatedCost());

        if(childrenData == null || childrenData.size() == 0) {
            return;
        }

        Operator child;
        double   childrenCost = 0.0;

        for(Map<String,Object> childData : childrenData) {
            child         = extractNode(childData, schema);
            childrenCost += child.getAccumulatedCost();

            plan.setChild(parent, child);
            extractChildNodes(plan, child, childData, schema);
        }

        parent.setCost(parent.getAccumulatedCost() - childrenCost);
    }

    /**
     * Extracts the data corresponding to node that is structured Jackson's Simple Data Binding 
     * format and creates an {@link Operator} object.
     *
     * The operator information that postgres' {@code EXPLAIN} produces contains at least:
     *
     * The complete list of attributes are explained <a 
     * href="http://archives.postgresql.org/pgsql-hackers/2010-11/msg00214.php">here</a> and can be extracted from 
     * PostgreSQL's source code, by looking to file {@code src/backend/commands/explain.c}
     *
     * If a schema object is given, the database objects that are referenced by a plan are bound to 
     * the operator.
     *
     * @param nodeData
     *     mapping of node attribute names and values.
     * @param schema
     *     can be null. A {@code Schema} where metadata of an object referred by an operator is 
     *     stored. If not null, it is used to bind operator references to actual metadata objects.
     * @return
     *     the {@code Operator} object containing the info that was extracted from {@code nodeData}.
     * @throws SQLException
     *     if {@code "Node Type"}, {@code "Total Cost"} or {@code "Plan Rows"} entries in the {@code 
     *     planData} map are empty or @{code null}. Also, when {@code schema} isn't null and the 
     *     metadata corresponding to a database object referred in a node is not found.
     */
    private static Operator extractNode(Map<String,Object> nodeData, Schema schema)
        throws SQLException
    {
        Object         type;
        Object         accCost;
        Object         cardinality;
        Object         dbObjectName;
        Operator       operator;
        DatabaseObject dbObject;

        type        = nodeData.get("Node Type");
        accCost     = nodeData.get("Total Cost");
        cardinality = nodeData.get("Plan Rows");

        if(type == null || accCost == null || cardinality == null) {
            throw new SQLException("Type, cost or cardinality is (are) null");
        }

        operator = new Operator((String) type, (Double) accCost, ((Number) cardinality).longValue());

        if( schema == null ) {
            return operator;
        }

        dbObjectName = nodeData.get("Relation Name");

        if(dbObjectName != null) {
            dbObject = schema.findTable((String)dbObjectName);

            if(dbObject == null) {
                throw new SQLException("Table " + dbObjectName + " not found in schema");
            }

            operator.add(dbObject);
        }

        dbObjectName = nodeData.get("Index Name");

        if(dbObjectName != null) {
            dbObject = schema.findIndex((String)dbObjectName);

            if(dbObject == null) {
                throw new SQLException("Index " + dbObjectName + " not found in schema");
            }

            operator.add(dbObject);
        }

        return operator;
    }
}
