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
package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.PGIndex;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.io.Reader;
import java.io.StringReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import static edu.ucsc.dbtune.util.Strings.compareVersion;

/**
 * The interface to the PostgreSQL optimizer.
 *
 * @author Ivo Jimenez
 */
public class PGOptimizer extends Optimizer
{
    private Connection connection;
    private Schema     schema;
    private boolean    obtainPlan;

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
        this.schema     = schema;
        this.connection = connection;

        if(schema == null) {
            obtainPlan = true;
        } else {
            String version = getVersion(connection);

            if(compareVersion("9.0.0", version) > 0) {
                throw new UnsupportedOperationException(
                        "PostgreSQL version " + version + " doesn't produce formatted EXPLAIN plans");
            }

            obtainPlan = false;
        }
    }

    /**
     * Creates an optimizer that doesn't obtain execution plans.
     *
     * @param connection
     *     JDBC connection
     */
    public PGOptimizer(Connection connection) throws SQLException
    {
        this(connection,null);

        this.obtainPlan = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedSQLStatement explain(SQLStatement sql, Configuration indexes) throws SQLException {
        ResultSet rs;
        Statement stmt;
        String    explainSql;
        String    indexOverhead;
        String[]  ohArray;
        double[]  maintCost;
        double    totalCost;

        explainSql = "EXPLAIN INDEXES " + explainIndexListString(indexes) + sql.getSQL();
        stmt       = connection.createStatement();
        rs         = stmt.executeQuery(explainSql);

        if(!rs.next()) {
            throw new SQLException("No result from EXPLAIN statement");
        }

        sql.setSQLCategory(SQLCategory.from(rs.getString("category")));
        indexOverhead = rs.getString("index_overhead");
        ohArray       = indexOverhead.split(" ");
        maintCost     = new double[indexes.size()];

        verifyOverheadArray(indexes.size(), ohArray);

        for(int i = 0; i < indexes.size(); i++){

            final String   ohString  = ohArray[i];
            final String[] splitVals = ohString.split("=");

            Checks.checkAssertion(splitVals.length == 2, "We got an unexpected result in index_overhead.");

            final int    id       = Integer.valueOf(splitVals[0]);
            final double overhead = Double.valueOf(splitVals[1]);

            maintCost[id] = overhead;
        }

        totalCost = Double.valueOf(rs.getString("qcost"));

        PreparedSQLStatement sqlStmt = new PreparedSQLStatement(sql,totalCost,maintCost,indexes,1);

        if(obtainPlan) {
            sqlStmt.setPlan(getPlan(connection,sql));
        }

        rs.close();
        stmt.close();

        return sqlStmt;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration recommendIndexes(SQLStatement sql) throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs   = stmt.executeQuery("RECOMMEND INDEXES " + sql.getSQL());
        int       id   = 0;

        List<Index> indexes = new ArrayList<Index>();

        while(rs.next()){
            touchIndexSchema(indexes, rs, id);
            ++id;
        }

        rs.close();
        stmt.close();

        return new Configuration(indexes);
    }

    /**
     * returns the version of the PostgreSQL instance that the given {@code connection} is 
     * communicating to.
     *
     * @param connection
     *     connection object from which the version will be retrieved from
     * @return
     *     a string containing the version number, e.g. "9.0.4"; "0.0.0" if not known
     * @throws SQLException
     *     if the underlying system is old enough such that it doesn't implement the {@code 
     *     version()} function; if another SQL error occurs while retrieving the system version.
     */
    public static String getVersion(Connection connection) throws SQLException {
        Statement st;
        ResultSet rs;
        String    version;

        st = connection.createStatement();
        rs = st.executeQuery("SELECT version()");

        version = "0.0.0";

        while(rs.next()) {
            version = rs.getString("version");
            version = version.substring(11,version.indexOf(" on "));
        }

        rs.close();
        st.close();

        return version;
    }

    private void touchIndexSchema(List<Index> candidateSet, ResultSet rs, int id) throws SQLException {
        final int       reloid = Integer.valueOf(rs.getString("reloid"));
        final boolean   isSync = rs.getString("sync").charAt(0) == 'Y';
        final List<Column> columns = new ArrayList<Column>();
        final String columnsString = rs.getString("atts");

        if(columnsString.length() > 0){
            for (String attnum  : columnsString.split(" ")){
                columns.add(new Column(Integer.valueOf(attnum)));
            }
        }

        // descending
        final List<Boolean> isDescending        = new ArrayList<Boolean>();
        final String        descendingString    = rs.getString("desc");
        if(descendingString.length() > 0){
            for (String desc : rs.getString("desc").split(" ")){
                isDescending.add(desc.charAt(0) == 'Y');
            }
        }

        final double        creationCost    = Double.valueOf(rs.getString("create_cost"));
        final double        megabytes       = Double.valueOf(rs.getString("megabytes"));

        final String indexName    = "sat_index_" + id;
        final String creationText = updateCreationText(rs, isSync, indexName);

        try {
            candidateSet.add(
                new PGIndex(
                    reloid, isSync, columns, isDescending, id,
                    megabytes, creationCost, creationText) );
        } catch(Exception ex) {
            throw new SQLException(ex);
        }
    }

    private String updateCreationText(ResultSet rs, boolean sync, String indexName) throws SQLException {
        String creationText = rs.getString("create_text");
        if (sync){
            creationText = creationText.replace(
                    "CREATE SYNCHRONIZED INDEX ?",
                    "CREATE SYNCHRONIZED INDEX " + indexName
                    );
        } else {
            creationText = creationText.replace(
                    "CREATE INDEX ?",
                    "CREATE INDEX " + indexName
                    );
        }
        return creationText;
    }

    private static void verifyOverheadArray(Integer cardinality, String[] ohArray) {
        if(cardinality == 0){
            // we expect ohArray to have one elt that is the empty string
            // but don't complain if it's empty
            if(ohArray.length != 0){
                Checks.checkAssertion(ohArray.length == 1, "Too many elements in ohArray.");
                Checks.checkAssertion(ohArray[0].length() == 0, "There is an unexpected element in ohArray.");
            }
        } else {
            Checks.checkAssertion(cardinality == ohArray.length, "Wrong length of ohArray.");
        }
    }

    /**
     * Returns a string containing a comma-separated list of the given indexes.
     *
     * @param indexes
     *     a string containing the PG-dependent string representation of the given list, as the 
     *     EXPLAIN INDEXES statement expects it
     */
    private static String explainIndexListString(Iterable<? extends Index> indexes) {
        // It's important that this method generates the string in the same order that 
        // Configuration.getIterator() produces the index list
        
        StringBuilder sb = new StringBuilder();

        sb.append("( ");

        for (Index idx : indexes) {
            sb.append(idx.getId()).append("(");
            if (idx.getScanOption() == Index.SYNCHRONIZED) {
                sb.append("synchronized ");
            }

            Table table = idx.getTable();
            sb.append(table.getId());

            for (int i = 0; i < idx.size(); i++) {
                sb.append(idx.getDescending().get(i) ? " desc" : " asc");
                final List<Column> cols = idx.getColumns();
                sb.append(" ").append(cols.get(i).getOrdinalPosition());
            }
            sb.append(") ");
        }
        sb.append(") ");

        return sb.toString();
    }

    /**
     * Returns the plan for the given statement
     *
     * @param connection
     *     connection to the DBMS
     * @param sql
     *     statement whose plan is retrieved
     * @return
     *     an execution plan for the given statement
     * @throws SQLException
     *     if something goes wrong while talking to the DBMS
     */
    protected SQLStatementPlan getPlan(Connection connection, SQLStatement sql) throws SQLException
    {
        String           explain = "EXPLAIN (COSTS true, FORMAT json) ";
        Statement        st      = connection.createStatement();
        ResultSet        rs      = st.executeQuery(explain + sql.getSQL());
        SQLStatementPlan plan    = null;
        int              cnt     = 0;

        while(rs.next()) {
            try {
                plan = parseJSON(new StringReader(rs.getString(1)), schema);
                cnt++;
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }

        if(cnt != 1) {
            throw new SQLException("Something wrong happened, got " + cnt + " plan(s)");
        }

        plan.setStatement(sql);
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
     *     if an error occurs when reading from data from {@code reader}
     * @throws SQLException
     *     when an error occurs during the parsing, eg. a data type conversion error occurs.
     */
    public static SQLStatementPlan parseJSON(Reader reader, Schema schema)
        throws IOException, SQLException
    {
        ObjectMapper     mapper;
        SQLStatementPlan plan;
        Operator         root;
        BufferedReader   breader;
        StringReader     sreader;
        String           line;
        String           sql;

        // get the SQL since we need it to instantiate SQLStatement objects
        breader = new BufferedReader(reader);
        sql     = "";

        while((line = breader.readLine()) != null) {
            sql += line + "\n";
        }

        // then create a string reader to pass it down to the JSON mapper
        sreader = new StringReader(sql);
        mapper  = new ObjectMapper();

        @SuppressWarnings("unchecked")
        List<Map<String,Object>> planData = mapper.readValue(sreader, List.class);

        if(planData == null) {
            return new SQLStatementPlan(new SQLStatement(sql, SQLCategory.UNKNOWN), new Operator());
        }

        if(planData.size() > 1) {
            throw new SQLException("More than one root node");
        }

        @SuppressWarnings("unchecked")
        Map<String,Object> rootData = (Map<String,Object>) planData.get(0).get("Plan");

        root = extractNode(rootData, schema);
        plan = new SQLStatementPlan(new SQLStatement(sql,SQLCategory.UNKNOWN), root);

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
    protected static void extractChildNodes(
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
