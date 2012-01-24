package edu.ucsc.dbtune.optimizer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.util.BitArraySet;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Interface to the DB2 optimizer.
 * <p>
 * This class assumes that the Explain Tables have been created in the target DB2 instance.
 *
 * @see <a href="http://bit.ly/vmHlsj">Explain Tables</a>
 *
 * @author Karl Schnaitter
 * @author Huascar Sanchez
 * @author Ivo Jimenez
 */
public class DB2Optimizer extends AbstractOptimizer
{
    private Connection connection;

    /**
     * Creates a DB2 optimizer with the given information.
     *
     * @param connection
     *     a live connection to DB2
     */
    public DB2Optimizer(Connection connection)
    {
        this.connection = connection;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ExplainedSQLStatement explain(SQLStatement sql, Set<Index> indexes)
        throws SQLException
    {
        Set<Index> used;
        Map<Index, Double> updateCosts;
        SQLStatementPlan plan;
        double selectCost;
        double updateCost;

        create(indexes, connection);

        plan = getPlan(sql, indexes);
        used = getUsedIndexes(catalog, connection);

        if (sql.getSQLCategory().isSame(SQLCategory.NOT_SELECT)) {
            updateCost = getUpdateCost(connection);
            updateCosts = new HashMap<Index, Double>();
            // XXX: issue #142
            // updateCosts = getUpdatedIndexes(connection);
        } else {
            updateCost = 0.0;
            updateCosts = new HashMap<Index, Double>();
        }

        selectCost = getCost(connection) - updateCost;

        return new ExplainedSQLStatement(
            sql, plan, this, selectCost, updateCost, updateCost, updateCosts, indexes, used, 1);     
    }

    /**
     * Clears the content from the explain tables, so that the information of only one statement is 
     * contained in them.
     *
     * @param connection
     *     connection used to communicate with the DBMS
     * @throws SQLException
     *      if an error occurs while operating over the {@code ADVISE_INDEX} table
     */
    private static void clearTables(Connection connection) throws SQLException
    {
        Statement stmt = connection.createStatement();

        stmt.executeUpdate("DELETE FROM SYSTOOLS.ADVISE_INDEX");
        stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_OBJECT");
        stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_OPERATOR");
        stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_PREDICATE");
        stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_STATEMENT");

        stmt.close();
    }

    /**
     * Loads the given configuration the {@code ADVISE_INDEX} table.
     *
     * @param configuration
     *     configuration being created
     * @param connection
     *     connection used to communicate with the DBMS
     * @throws SQLException
     *      if an error occurs while operating over the {@code ADVISE_INDEX} table
     */
    private static void create(Set<Index> configuration, Connection connection) throws SQLException
    {
        clearTables(connection);

        if (configuration.isEmpty())
            return;

        Statement stmt = connection.createStatement();

        for (Index index : configuration)
            stmt.execute(getAdviseIndexInsertStatement(index));

        stmt.close();
    }

    /**
     * Returns a SQL statement that can be executed to insert the given index into the {@code 
     * ADVISE_INDEX} table.
     *
     * @param index
     *     an index
     * @return
     *     a string containing the  string representation of the given index
     */
    private static String getAdviseIndexInsertStatement(Index index)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO systools.advise_index (");

        // user metadata... extract it from the system's recommended indexes 
        sb.append("EXPLAIN_REQUESTER, ");
        sb.append("TBCREATOR, "); 
        
        // table name (string) 
        sb.append("TBNAME, ");

        // '+A-B+C' means "A" ASC, "B" DESC, "C" ASC ...not sure about INCLUDE columns
        sb.append("COLNAMES, ");

        // #Key columns + #Include columns. Must match COLNAMES
        sb.append("COLCOUNT, ");

        // 'P' (primary), 'D' (duplicates allowed), 'U' (unique) 
        sb.append("UNIQUERULE, ");

        // IF unique index THEN #Key columns ELSE -1 
        sb.append("UNIQUE_COLCOUNT, ");

        // 'Y' or 'N' indicating if reverse scans are supported
        sb.append("REVERSE_SCANS, ");

        // 'CLUS', 'REG', 'DIM', 'BLOK' 
        sb.append("INDEXTYPE, ");
        
        // The name of the index and the CREATE INDEX statement (must match)
        sb.append("NAME, ");  
        sb.append("CREATION_TEXT, ");
        
        // Indicates if the index is real or hypothetical
        // 'Y' or 'N' 
        sb.append("EXISTS, ");
        
        // Indicates if the index is system defined... should only be true for real indexes
        // 0, 1, or 2 
        sb.append("SYSTEM_REQUIRED, ");
        
        // We use this field to identify an index (also stored locally)
        sb.append("IID, ");
        
        // enable the index for what-if analysis
        // 'Y' or 'N'
        sb.append("USE_INDEX, ");
        
        // statistics, set to -1 to indicate unknown
        sb.append("NLEAF, ");
        sb.append("NLEVELS, ");
        sb.append("FIRSTKEYCARD, ");
        sb.append("FULLKEYCARD, ");
        sb.append("CLUSTERRATIO, ");
        sb.append("AVGPARTITION_CLUSTERRATIO, ");
        sb.append("AVGPARTITION_CLUSTERFACTOR, ");
        sb.append("AVGPARTITION_PAGE_FETCH_PAIRS, ");
        sb.append("DATAPARTITION_CLUSTERFACTOR, ");
        sb.append("CLUSTERFACTOR, ");
        sb.append("SEQUENTIAL_PAGES, ");
        sb.append("DENSITY, ");
        sb.append("FIRST2KEYCARD, ");
        sb.append("FIRST3KEYCARD, ");
        sb.append("FIRST4KEYCARD, ");
        sb.append("PCTFREE, ");

        // empty string instead of -1 for this one
        sb.append("PAGE_FETCH_PAIRS, ");
        // 0 instead of -1 for this one
        sb.append("MINPCTUSED, ");
        
        /* the rest are likely useless */
        sb.append("EXPLAIN_TIME, ");
        sb.append("CREATE_TIME, ");
        sb.append("STATS_TIME, ");
        sb.append("SOURCE_NAME, ");
        sb.append("REMARKS, ");
        sb.append("CREATOR, ");
        sb.append("DEFINER, ");
        sb.append("SOURCE_SCHEMA, ");
        sb.append("SOURCE_VERSION, ");
        sb.append("EXPLAIN_LEVEL, ");
        sb.append("USERDEFINED, ");
        sb.append("STMTNO, ");
        sb.append("SECTNO, ");
        sb.append("QUERYNO, ");
        sb.append("QUERYTAG, ");
        sb.append("PACKED_DESC, ");
        sb.append("RUN_ID, ");
        sb.append("RIDTOBLOCK, ");
        sb.append("CONVERTED) VALUES (");

        /////////////////////////
        // values
        /////////////////////////

        sb.append("'DBTune', ");
        sb.append("'" + index.getTable().getSchema().getName() + "', ");
        sb.append("'" + index.getTable().getName() + "', ");
        sb.append("'" + getColumnNames(index) + "', ");
        sb.append(index.size() + ", ");

        if (index.isPrimary())
            sb.append("'P', ");
        else if (index.isUnique())
            sb.append("'U', ");
        else
            sb.append("'D', ");

        if (index.isUnique())
            sb.append(index.size() + ", ");
        else
            sb.append("-1, ");

        if (index.isReversible())
            sb.append("'Y', ");
        else
            sb.append("'N', ");

        if (index.isClustered())
            sb.append("'CLUS', ");
        else
            sb.append("'REG', ");

        sb.append("'" + index.getName() + "', ");
        sb.append("'CREATE INDEX " + index.getName() + "', ");
        sb.append("'N', ");
        sb.append("0, ");
        sb.append(index.getId() + ", ");
        sb.append("'Y', ");
        sb.append("-1, ");
        sb.append("-1, ");
        sb.append("-1, ");
        sb.append("-1, ");
        sb.append("-1, ");
        sb.append("-1, ");
        sb.append("-1, ");
        sb.append("'', ");
        sb.append("-1, ");
        sb.append("-1, ");
        sb.append("-1, ");
        sb.append("-1, ");
        sb.append("-1, ");
        sb.append("-1, ");
        sb.append("-1, ");
        sb.append("-1, ");
        sb.append("'', ");
        sb.append("0 , ");
        sb.append("CURRENT TIMESTAMP, ");
        sb.append("CURRENT TIMESTAMP, ");
        sb.append("NULL, ");
        sb.append("'DBTune', ");
        sb.append("'Created by DBTune', ");
        sb.append("'SYSTEM', ");
        sb.append("'SYSTEM', ");
        sb.append("'NULLID', ");
        sb.append("'', ");
        sb.append("'P', ");
        sb.append("1, ");
        sb.append("1, ");
        sb.append("1, ");
        sb.append("1, ");
        sb.append("'', ");
        sb.append("NULL, ");
        sb.append("NULL, ");
        sb.append("'N', ");
        sb.append("'Z')");

        return sb.toString();
    }

    /**
     * Returns a comma-separated list of the names of columns contained in the given index.
     *
     * @param index
     *     an index
     * @return
     *     a string containing the comma-separated list of column names
     */
    private static String getColumnNames(Index index)
    {
        StringBuilder sb = new StringBuilder();
        boolean first = true;

        for (Column col : index.columns()) {
            if (first)
                first = false;
            else
                sb.append(",");

            if (index.isAscending(col))
                sb.append("+");
            else
                sb.append("-");

            sb.append(col.getName());
        }

        return sb.toString();
    }

    /**
     * Returns the cost of the statement that has been just explained. Assumes that only one 
     * statement is contained in the {@code EXPLAIN_STATEMENT} table.
     *
     * @param connection
     *     connection used to communicate with the DBMS
     * @return
     *     the cost of the plan
     * @throws SQLException
     *     if something goes wrong while talking to the DBMS
     */
    private static double getCost(Connection connection) throws SQLException
    {
        Statement stmt = connection.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                "SELECT SUM(total_cost) as cost " +
                " FROM  systools.explain_statement " +
                " WHERE explain_level='P'");

        if (!rs.next())
            throw new SQLException("No result in EXPLAIN_STATEMENT table");

        double cost = rs.getDouble("cost");

        rs.close();
        stmt.close();
        
        return cost;
    }

    /**
     * Explains the given statement and returns its execution plan.
     *
     * @param sql
     *     statement which the plan is obtained for
     * @return
     *     an execution plan
     * @throws SQLException
     *     if something goes wrong while talking to the DBMS
     */
    protected SQLStatementPlan getPlan(SQLStatement sql, Set<Index> indexes) throws SQLException
    {
        Statement stmt = connection.createStatement();
        ResultSet rs;
        SQLStatementPlan plan;

        stmt.execute("SET CURRENT EXPLAIN MODE = EVALUATE INDEXES");
        stmt.execute(sql.getSQL());
        stmt.execute("SET CURRENT EXPLAIN MODE = NO");

        rs = stmt.executeQuery(
                "SELECT " +
                "     o.operator_id as node_id, " +
                "     s2.target_id as parent_id, " +
                "     o.operator_type as operator_name, " +
                "     s1.object_schema as object_schema, " +
                "     s1.object_name as object_name, " +
                "     s2.stream_count as cardinality, " +
                "     o.total_cost as cost " +
                " FROM " +
                "     systools.explain_operator o " +
                "        LEFT OUTER JOIN " +
                "     systools.explain_stream s2 " +
                "           ON o.operator_id=s2.source_id AND " +
                "              s2.target_id > 0 " +
                "        LEFT OUTER JOIN " +
                "     systools.explain_stream s1 " +
                "           ON o.operator_id = s1.target_id AND " +
                "              o.explain_time = s1.explain_time AND " +
                "              s1.object_name IS NOT NULL " +
                " ORDER BY " +
                "     o.explain_time ASC, " +
                "     o.operator_id ASC");

        plan = parsePlan(catalog, rs, indexes);

        rs.close();
        stmt.close();

        return plan;
    }

    /**
     * Returns the update cost of the statement that has been just explained. It assumes that the 
     * type of statement has been checked already, i.e. that the statement contained in {@code 
     * EXPLAIN_STATEMENT} table is of type {@link SQLCategory#NOT_SELECT}.
     *
     * @param connection
     *     connection used to communicate with the DBMS
     * @return
     *     the cost of the plan
     * @throws SQLException
     *     if something goes wrong while talking to the DBMS
     */
    private static double getUpdateCost(Connection connection) throws SQLException
    {
        Statement stmt = connection.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                    "SELECT    total_cost " +
                    " FROM     systools.explain_operator " +
                    " WHERE    operator_id = 2 OR operator_id = 3 " +
                    " ORDER BY operator_id");

        if (!rs.next())
            throw new SQLException("Could not get update cost: no rows");

        double updateOpCost = rs.getDouble(1);

        if (!rs.next())
            throw new SQLException("Could not get update cost: only one row");

        double childOpCost = rs.getDouble(1);

        if (rs.next())
            throw new SQLException("Could not get update cost: too many rows");

        rs.close();
        stmt.close();
        
        return updateOpCost - childOpCost;
    }

    /**
     * Returns the set of indexes used in the execution plan of the statement that has been 
     * previously explained. Assumes that the {@code EXPLAIN_OBJECT} table contains information 
     * corresponding to only one statement.
     *
     * @param connection
     *     connection used to communicate with the DBMS
     * @param catalog
     *     used to retrieve metadata information
     * @return
     *     the set of indexes used by the execution plan
     * @throws SQLException
     *     if something goes wrong while talking to the DBMS
     */
    private static Set<Index> getUsedIndexes(
            Catalog catalog, Connection connection)
        throws SQLException
    {
        Set<Index> used = new HashSet<Index>();
        Statement stmt = connection.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                    "SELECT object_name as name" +
                    " FROM  systools.explain_object " +
                    " WHERE object_type = 'IX'");

        while (rs.next())
            used.add(catalog.findIndex(rs.getString("name").trim()));

        rs.close();
        stmt.close();
        
        return used;
    }

    /**
     * Reads the content of the {@code ADVISE_INDEX}, after a {@code RECOMMEND INDEXES} operation 
     * has been done. Creates one index per each record in the table. 
     *
     * @param connection
     *     connection used to communicate with the DBMS
     * @param catalog
     *     used to retrieve metadata information
     * @return
     *     the cost of the plan
     * @throws SQLException
     *     if something goes wrong while talking to the DBMS
     */
    private static Set<Index> getRecommendedIndexes(Catalog catalog, Connection connection)
        throws SQLException
    {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM systools.advise_index");
        Set<Index> recommended = new HashSet<Index>();
        List<Column> columns;
        List<Boolean> descending;
        Schema schema;
        Table table;
        Index index;
        String name;
        boolean unique = false;
        boolean clustered = false;
        boolean primary = false;

        while (rs.next()) {
            schema = catalog.findSchema(rs.getString("tbcreator").trim());
            table = schema.findTable(rs.getString("tbname").trim());
            columns = new ArrayList<Column>();
            descending = new ArrayList<Boolean>();
            name = "sat_index_" + new Random().nextInt(10000);

            parseColumnNames(table, rs.getString("colnames"), columns, descending);

            if (rs.getString("uniquerule").trim().equals("U"))
                unique = true;
            else if (rs.getString("uniquerule").trim().equals("D"))
                unique = false;
            else {
                unique = true;
                primary = true;
            }

            if (rs.getString("indextype").trim().equals("REG"))
                primary = false;
            else if (rs.getString("indextype").trim().equals("CLUS"))
                clustered = true;
            else
                clustered = false;

            index = new Index(name, columns, descending, primary, unique, clustered);

            recommended.add(index);
        }

        rs.close();
        stmt.close();

        return recommended;
    }

    /**
     * Parses the {@code ADVISE_INDEX.COLNAMES} column in order to extract the list of referenced 
     * columns and their corresponding ASC/DESC values. The read values are placed in the {@code 
     * columns} and {@code descending} lists sent as arguments.
     *
     * @param table
     *     table from which the columns belong to
     * @param str
     *     string being parsed
     * @param columns
     *     list being populated with the name of columns
     * @param descending
     *     list being populated with the asc/desc values. The size of the list is equal to the 
     *     {@code columns} list
     * @throws SQLException
     *     if a SQL communication error occurs
     */
    private static void parseColumnNames(
            Table table, String str, List<Column> columns, List<Boolean> descending)
        throws SQLException
    {
        char c;
        int nameStart;
        boolean newColumn;
        
        c = str.charAt(0);

        if (c == '+')
            descending.add(false);
        else if (c == '-')
            descending.add(true);
        else
            throw new SQLException("first character '" + c + "' unexpected in COLNAMES");

        // name starts after +/- symbol
        nameStart = 1;
        
        for (int i = 1; i < str.length(); i++) {
            c = str.charAt(i);

            if (c == '+') {
                descending.add(false);
                newColumn = true;
            } else if (c == '-') {
                descending.add(true);
                newColumn = true;
            } else
                newColumn = false;
            
            if (newColumn) {
                if (i - nameStart < 1)
                    throw new SQLException("empty column name found in ADVISE_INDEX.COLNAMES");
                columns.add(table.findColumn(str.substring(nameStart, i)));
                nameStart = i + 1;
            }
        }
        
        if (str.length() - nameStart < 1)
            throw new SQLException("empty column name found in ADVISE_INDEX.COLNAMES");

        columns.add(table.findColumn(str.substring(nameStart, str.length())));
    }

    /**
     * Reads an entry in the given result set object and creates a {@link Operator} object out of 
     * it.
     *
     * @param rs
     *     result set from which a record is read
     * @param catalog
     *     used to retrieve metadata information
     * @return
     *     the execution plan
     * @throws SQLException
     *     if an expected column is not in the result set; if a database object referenced by the 
     *     operator can't be found in the catalog.
     */
    static Operator parseNode(Catalog catalog, ResultSet rs, Set<Index> indexes) throws SQLException
    {
        double accomulatedCost = rs.getDouble("cost");
        long cardinality = rs.getLong("cardinality");
        String name = rs.getString("operator_name");
        String dboName = rs.getString("object_name");
        String dboSchema = rs.getString("object_schema");
        DatabaseObject dbo;

        Operator op = new Operator(name.trim(), accomulatedCost, cardinality);

        if (dboSchema == null || dboName == null)
            return op;

        dboSchema = dboSchema.trim();
        dboName = dboName.trim();

        if (dboSchema.equals("SYSTEM"))
            // SYSTEM means that it's in the ADVISE_INDEX table; at least that's what we can infer
            dbo = catalog.findIndex(dboName);
        else
            dbo = catalog.findByQualifiedName(dboSchema + "." + dboName);

        if (dbo == null)
            // try to find it in the set of indexes passed in the what-if invocation
            dbo = find(indexes, dboName);

        if (dbo == null)
            throw new SQLException("Can't find object " + dboSchema + "." + dboName);

        op.add(dbo);

        return op;
    }

    /**
     * Finds an index by name in a set of indexes.
     *
     * @param indexes
     *      set of indexes where one with the given name is being looked for
     * @param name
     *      name of the index being looked for
     * @return
     *      the index with the given name; {@code null} if not found
     */
    private static Index find(Set<Index> indexes, String name)
    {
        for (Index i : indexes) {
            if (i.getName().equals(name))
                return i;
        }

        return null;
    }

    /**
     * Reads the {@code EXPLAIN_OPERATOR} table and creates a {@link SQLStatementPlan} object out of 
     * it. Assumes that the {@code EXPLAIN_OPERATOR} and {@code EXPLAIN_STREAM} tables contain 
     * entries corresponding to one statement.
     *
     * @param rs
     *     result set object that contains the answer from the DBMS for the query that extracts 
     *     operator information. It is assumed that the set contains information for a single 
     *     statement.
     * @param catalog
     *     used to retrieve metadata information
     * @return
     *     the execution plan
     * @throws SQLException
     *     if something goes wrong while talking to the DBMS
     */
    static SQLStatementPlan parsePlan(Catalog catalog, ResultSet rs, Set<Index> indexes)
        throws SQLException
    {
        Map<Integer, Operator> idToNode;
        Operator child;
        Operator parent;
        Operator root;
        SQLStatementPlan plan;

        if (!rs.next())
            throw new SQLException("Empty plan");

        root = parseNode(catalog, rs, indexes);
        plan = new SQLStatementPlan(root);
        idToNode = new HashMap<Integer, Operator>();

        idToNode.put(1, root);

        while (rs.next()) {
            child = parseNode(catalog, rs, indexes);
            parent = idToNode.get(rs.getInt("parent_id"));

            if (parent == null)
                throw new SQLException(child + " expecting parent_id=" + rs.getInt("parent_id"));

            plan.setChild(parent, child);
            idToNode.put(rs.getInt("node_id"), child);
        }

        rs.close();

        return plan;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> recommendIndexes(SQLStatement sql) throws SQLException
    {
        Statement stmt = connection.createStatement();

        clearTables(connection);

        stmt.execute("SET CURRENT EXPLAIN MODE = RECOMMEND INDEXES");
        stmt.execute(sql.getSQL());
        stmt.execute("SET CURRENT EXPLAIN MODE = NO");

        stmt.close();

        return getRecommendedIndexes(catalog, connection);
    }
}
