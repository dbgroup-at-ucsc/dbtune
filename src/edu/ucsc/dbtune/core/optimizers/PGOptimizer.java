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
package edu.ucsc.dbtune.core.optimizers;

import edu.ucsc.dbtune.core.optimizers.plan.SQLStatementPlan;
import edu.ucsc.dbtune.core.optimizers.plan.Operator;

import java.io.Reader;
import java.io.StringReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Map;
import java.util.List;
import java.lang.ClassCastException;

import org.codehaus.jackson.map.ObjectMapper;

import static edu.ucsc.dbtune.util.Strings.compareVersion;
import static edu.ucsc.dbtune.core.metadata.PGCommands.getVersion;

/**
 * {@inheritDoc}
 */
public class PGOptimizer extends Optimizer
{
    private Connection connection;
    private String     explain;

    /**
     * Creates a new optimizer for PostgreSQL systems.
     *
     * @param connection
     *     JDBC connection used to communicate to a PostgreSQL system.
     * @throws SQLException
     *     if an error occurs while communicating to the server.
     * @throws UnsupportedOperationException
     *     if the version of the PostgreSQL {@code connection} is communicating with isn't 9.0.0 or 
     *     above.
     */
    public PGOptimizer(Connection connection) throws SQLException, UnsupportedOperationException {
        String version = getVersion(connection);

        if(compareVersion("9.0.0", version) > 0) {
            throw new UnsupportedOperationException(
                "PostgreSQL version " + version + " doesn't produce formatted EXPLAIN plans");
        }

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
                plan = parseJSON(new StringReader(rs.getString(1)));
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
     * @return
     *     the object representing the plan for the statement contained in the reader's source.
     * @throws IOException
     *     if an error occurs when reading from source
     */
    public static SQLStatementPlan parseJSON(Reader reader)
        throws IOException, ClassCastException, Exception
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
            throw new Exception("More than one root node");
        }

        @SuppressWarnings("unchecked")
        Map<String,Object> rootData = (Map<String,Object>) planData.get(0).get("Plan");

        root = extractNode(rootData);
        plan = new SQLStatementPlan(root);

        extractChildNodes(plan, root, rootData);

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
     * @see <a href="http://wiki.fasterxml.com/JacksonDocumentation">Jackson Documentation</a>
     */
    private static void extractChildNodes(SQLStatementPlan plan, Operator parent, Map<String,Object> parentData )
        throws ClassCastException, Exception
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
            child         = extractNode(childData);
            childrenCost += child.getAccumulatedCost();

            plan.setChild(parent, child);
            extractChildNodes(plan, child, childData);
        }

        parent.setCost(parent.getAccumulatedCost() - childrenCost);
    }

    /**
     * Extracts the data corresponding to node that is structured Jackson's Simple Data Binding 
     * format and creates an {@link Operator} object.
     *
     * @param nodeData
     *     mapping of node attribute names and values.
     * @return
     *     the {@link Operator} object containing the info from {@link planData}.
     * @throws Exception
     *     if {@code "Node Type"}, {@code "Total Cost"} or {@code "Plan Rows"} entries in the {@code 
     *     planData} map are empty or @{code null}.
     */
    private static Operator extractNode(Map<String,Object> nodeData) throws Exception {
        Object type;
        Object accCost;
        Object cardinality;

        type        = nodeData.get("Node Type");
        accCost     = nodeData.get("Total Cost");
        cardinality = nodeData.get("Plan Rows");

        if(type == null || accCost == null || cardinality == null) {
            throw new Exception("Type, cost or cardinality is (are) null");
        }

        return new Operator((String) type, (Double) accCost, ((Number) cardinality).longValue());
    }
}
