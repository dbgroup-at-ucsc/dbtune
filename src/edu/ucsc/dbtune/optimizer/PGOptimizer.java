package edu.ucsc.dbtune.optimizer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.codehaus.jackson.map.ObjectMapper;

import static edu.ucsc.dbtune.metadata.Index.NON_UNIQUE;
import static edu.ucsc.dbtune.metadata.Index.SECONDARY;
import static edu.ucsc.dbtune.metadata.Index.SYNCHRONIZED;
import static edu.ucsc.dbtune.metadata.Index.UNCLUSTERED;
import static edu.ucsc.dbtune.util.Strings.compareVersion;
import static edu.ucsc.dbtune.util.Strings.toBooleanList;
import static edu.ucsc.dbtune.util.Strings.toDoubleArrayFromIndexed;
import static edu.ucsc.dbtune.util.Strings.toIntegerArray;

/**
 * The interface to the PostgreSQL optimizer.
 *
 * @author Ivo Jimenez
 */
public class PGOptimizer extends AbstractOptimizer
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

        if (schema == null) {
            obtainPlan = true;
        } else {
            String version = getVersion(connection);

            if (compareVersion("9.0.0", version) > 0) {
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
     *      JDBC connection
     * @throws SQLException
     *      if an error occurs
     */
    public PGOptimizer(Connection connection) throws SQLException
    {
        this(connection, null);

        this.obtainPlan = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExplainedSQLStatement explain(SQLStatement sql, Set<Index> indexes)
        throws SQLException
    {
        ResultSet           rs;
        SQLStatementPlan    sqlPlan;
        Set<Index>          usedConf;
        List<Index>         list;
        Statement           stmt;
        String              indexPositions;
        String              indexOverhead;
        Map<Index, Double>  updateCost;
        double[]            updateCosts;
        double              selectCost;
        int[]               positions;

        stmt = connection.createStatement();
        list = new ArrayList<Index>(indexes);
        rs   = stmt.executeQuery("EXPLAIN INDEXES " + toString(list) + " " + sql.getSQL());

        if (!rs.next())
            throw new SQLException("No result from EXPLAIN statement");

        if (!sql.getSQLCategory().isSame(SQLCategory.from(rs.getString("category"))))
            throw new SQLException(
                    sql.getSQLCategory() + " not the same to " + 
                    SQLCategory.from(rs.getString("category")));

        selectCost     = rs.getDouble("qcost");
        indexPositions = rs.getString("indexes").trim();
        indexOverhead  = rs.getString("index_overhead").trim();

        rs.close();
        stmt.close();

        if (indexes.size() > 0) {
            updateCosts = toDoubleArrayFromIndexed(indexOverhead.split(" "), "=");

            if (!indexPositions.equals(""))
                positions = toIntegerArray(indexPositions.split(" "));
            else
                positions = new int[0];

            usedConf = getUsedConfiguration(list, positions);

        } else {
            usedConf   = new IndexBitSet<Index>();
            updateCosts = new double[0];
        }

        if (updateCosts.length != indexes.size())
            throw new SQLException(
                updateCosts.length + " update costs for " + indexes.size() + "indexes");

        updateCost = new HashMap<Index, Double>();

        for (int i = 0; i < updateCosts.length; i++) {
            updateCost.put(list.get(0), updateCosts[i]);
        }

        sqlPlan = null;

        if (obtainPlan)
            sqlPlan = getPlan(connection, sql);

        return new ExplainedSQLStatement(
                sql, sqlPlan, this, selectCost, updateCost, indexes, usedConf, 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> recommendIndexes(SQLStatement sql) throws SQLException
    {
        List<Column> columns;
        List<Index> indexes;
        List<Boolean> isDesc;
        Statement stmt;
        ResultSet rs;
        Table     table;
        Index     index;
        boolean   isSync;
        int[]     positions;
        String    indexName;

        indexes = new ArrayList<Index>();
        stmt    = connection.createStatement();
        rs      = stmt.executeQuery("RECOMMEND INDEXES " + sql.getSQL());

        while (rs.next()) {

            table = null;

            for (Schema sch : catalog) {
                table = sch.findTable(rs.getInt("reloid"));

                if (table != null)
                    break;
            }

            if (table == null)
                throw new SQLException("Can't find table with id " + rs.getInt("reloid"));

            isSync    = rs.getString("sync").charAt(0) == 'Y';
            positions = toIntegerArray(rs.getString("atts").split(" "));
            isDesc    = toBooleanList(rs.getString("desc").split(" "));
            columns   = getReferencedColumns(table, positions);
            indexName = "sat_index_" + new Random().nextInt(10000);

            index = new Index(indexName, columns, isDesc, SECONDARY, NON_UNIQUE, UNCLUSTERED);

            if (isSync)
                index.setScanOption(SYNCHRONIZED);

            index.setCreationCost(rs.getDouble("create_cost"));
            index.setBytes(rs.getLong("megabytes") * 1024 * 1024);

            indexes.add(index);
        }

        rs.close();
        stmt.close();

        return new IndexBitSet<Index>(indexes);
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
    public static String getVersion(Connection connection) throws SQLException
    {
        Statement st;
        ResultSet rs;
        String    version;

        st = connection.createStatement();
        rs = st.executeQuery("SELECT version()");

        version = "0.0.0";

        // sample output:
        //
        // test=# select version();
        //
        //                                           version
        //  ----------------------------------------------------------------------------------------
        //  PostgreSQL 8.3.0 on i686-pc-linux-gnu, compiled by GCC gcc (Ubuntu 4.4.3-4ubuntu5) 4.4.3
        //  (1 row)

        while (rs.next()) {
            version = rs.getString("version");
            version = version.substring(11, version.indexOf(" on "));
        }

        rs.close();
        st.close();

        return version;
    }

    /**
     * Returns the set of indexes referred by the array of integers, where each element correspond 
     * to the ordinal position of the {@link Index} contained in the configuration.
     *
     * @param indexes
     *     referred configuration
     * @param positions
     *     integers referring to the ordinal position of {@link Index} objects contained in the 
     *     configuration.
     * @return
     *      the referred index set
     * @see Configuration#getOrdinalPosition
     */
    private static Set<Index> getUsedConfiguration(List<Index> indexes, int[] positions)
    {
        Set<Index> usedConf = new IndexBitSet<Index>();

        for (int position : positions)
            usedConf.add(indexes.get(position));

        return usedConf;
    }

    /**
     * Returns the set of columns referred by the array of integers, where each element correspond 
     * to the ordinal position of the {@link Column} contained in the given table.
     *
     * @param table
     *     referred table
     * @param positions
     *     integers referring to the ordinal position of {@link Column} objects contained in the 
     *     configuration.
     * @return
     *      set of columns referred by the given array of integers
     * @throws SQLException
     *      if a column with an index can't be found
     * @see Table#getColumns
     */
    private static List<Column> getReferencedColumns(Table table, int[] positions)
        throws SQLException
    {
        List<Column> columns = new ArrayList<Column>();

        Column col;

        for (int position : positions) {
            col = (Column) table.at(position - 1);

            if (col == null)
                throw new SQLException(
                        "Can't find column with position " + position + " in table " + table);

            columns.add(col);
        }

        return columns;
    }

    /**
     * Returns a string containing a comma-separated list of the given indexes.
     *
     * @param indexes
     *     list of indexes
     * @return
     *     a string containing the PG-dependent string representation of the given list, as the 
     *     EXPLAIN INDEXES statement expects it
     * @throws SQLException
     *      if something goes wrong
     */
    private static String toString(List<Index> indexes) throws SQLException
    {
        // It's important that this method generates the string in the same order that 
        // Configuration.iterator() produces the index list
        
        StringBuilder sb = new StringBuilder();
        int position = 0;

        sb.append("( ");

        for (Index idx : indexes) {
            sb.append(position++).append("(");
            if (idx.getScanOption() == Index.SYNCHRONIZED) {
                sb.append("synchronized ");
            }

            sb.append(idx.getTable().getInternalID());

            for (Column col : idx.columns()) {
                sb.append(idx.isDescending(col) ? " desc" : " asc");
                sb.append(" ").append(col.getOrdinalPosition());
            }
            sb.append(") ");
        }
        sb.append(") ");

        return sb.toString();
    }

    /**
     * Returns the plan for the given statement.
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
    protected SQLStatementPlan getPlan(Connection connection, SQLStatement sql)
        throws SQLException
    {
        String           explain = "EXPLAIN (COSTS true, FORMAT json) ";
        Statement        st      = connection.createStatement();
        ResultSet        rs      = st.executeQuery(explain + sql.getSQL());
        SQLStatementPlan plan    = null;
        int              cnt     = 0;

        while (rs.next()) {
            try {
                plan = parseJSON(new StringReader(rs.getString(1)), schema);
                cnt++;
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }

        if (cnt != 1) {
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

        breader = new BufferedReader(reader);
        mapper  = new ObjectMapper();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> planData = mapper.readValue(breader, List.class);

        if (planData == null) {
            return new SQLStatementPlan(new Operator());
        }

        if (planData.size() > 1) {
            throw new SQLException("More than one root node");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> rootData = (Map<String, Object>) planData.get(0).get("Plan");

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
    protected static void extractChildNodes(
            SQLStatementPlan    plan,
            Operator            parent,
            Map<String, Object> parentData,
            Schema              schema)
        throws SQLException
    {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> childrenData =
            (List<Map<String, Object>>) parentData.get("Plans");

        parent.setCost(parent.getAccumulatedCost());

        if (childrenData == null || childrenData.size() == 0) {
            return;
        }

        Operator child;
        double   childrenCost = 0.0;

        for (Map<String, Object> childData : childrenData) {
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
     * href="http://archives.postgresql.org/pgsql-hackers/2010-11/msg00214.php">here</a> and can be 
     * extracted from PostgreSQL's source code, by looking to file {@code 
     * src/backend/commands/explain.c}
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
    private static Operator extractNode(Map<String, Object> nodeData, Schema schema)
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

        if (type == null || accCost == null || cardinality == null) {
            throw new SQLException("Type, cost or cardinality is (are) null");
        }

        operator = new Operator((String) type, (Double) accCost, ((Number) cardinality).longValue());

        if (schema == null) {
            return operator;
        }

        dbObjectName = nodeData.get("Relation Name");

        if (dbObjectName != null) {
            dbObject = schema.findTable((String) dbObjectName);

            if (dbObject == null) {
                throw new SQLException("Table " + dbObjectName + " not found in schema");
            }

            operator.add(dbObject);
        }

        dbObjectName = nodeData.get("Index Name");

        if (dbObjectName != null) {
            dbObject = schema.findIndex((String) dbObjectName);

            if (dbObject == null) {
                throw new SQLException("Index " + dbObjectName + " not found in schema");
            }

            operator.add(dbObject);
        }

        return operator;
    }
}
