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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.plan.InterestingOrder;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.Predicate;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Sets.newHashSet;

import static edu.ucsc.dbtune.util.MetadataUtils.find;

/**
 * Interface to the DB2 optimizer.
 * <p>
 * This class assumes that the {@code EXPLAIN_*}, {@code ADVISE_*} and {@code OPT_PROFILE} tables 
 * have been created in the target DB2 instance.
 *
 * @see <a href="http://bit.ly/vmHlsj">Explain Tables</a>
 * @see <a href="http://bit.ly/xxPpoz">OPT_PROFILE table</a>
 *
 * @author Karl Schnaitter
 * @author Huascar Sanchez
 * @author Ivo Jimenez
 * @author Quoc Trung Tran
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

        clearAdviseAndExplainTables(connection);

        insertIntoAdviseIndexTable(connection, indexes);

        plan = getPlan(connection, sql, catalog, indexes);
        used = newHashSet(plan.getIndexes());

        if (sql.getSQLCategory().isSame(SQLCategory.NOT_SELECT)) {
            updateCost = getUpdateCost(plan);
            updateCosts = new HashMap<Index, Double>();
            // XXX: issue #142
            // updateCosts = getUpdatedIndexes(connection);
        } else {
            updateCost = 0.0;
            updateCosts = new HashMap<Index, Double>();
        }

        selectCost = plan.getRootOperator().getAccumulatedCost() - updateCost;

        return new ExplainedSQLStatement(
            sql, plan, this, selectCost, updateCost, updateCost, updateCosts, indexes, used, 1);     
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> recommendIndexes(SQLStatement sql) throws SQLException
    {
        Statement stmt = connection.createStatement();

        clearAdviseAndExplainTables(connection);

        stmt.execute("SET CURRENT EXPLAIN MODE = RECOMMEND INDEXES");
        stmt.execute(sql.getSQL());
        stmt.execute("SET CURRENT EXPLAIN MODE = NO");

        stmt.close();

        return readAdviseIndexTable(connection, catalog);
    }

    /**
     * Returns a comma-separated list of the names of columns contained in the given index.
     *
     * @param index
     *     an index
     * @return
     *     a string containing the comma-separated list of column names
     */
    static String buildColumnNamesValue(Index index)
    {
        StringBuilder sb = new StringBuilder();

        for (Column col : index.columns()) {
            if (index.isAscending(col))
                sb.append("+");
            else
                sb.append("-");

            sb.append(col.getName());
        }

        return sb.toString();
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
    static String buildAdviseIndexInsertStatement(Index index)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(INSERT_INTO_ADVISE_INDEX_COLUMNS);
        sb.append("'" + index.getTable().getSchema().getName() + "', ");
        sb.append("'" + index.getTable().getName() + "', ");
        sb.append("'" + buildColumnNamesValue(index) + "', ");
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

        sb.append(INSERT_INTO_ADVISE_INDEX_REST_OF_VALUES);

        return sb.toString();
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
    static void clearAdviseAndExplainTables(Connection connection) throws SQLException
    {
        Statement stmt = connection.createStatement();

        stmt.executeUpdate(DELETE_FROM_ADVISE_INDEX);
        stmt.executeUpdate(DELETE_FROM_EXPLAIN_INSTANCE);
        /*
         * there's a trigger on EXPLAIN_INSTANCE that causes all data corresponding to an explain 
         * instance to be removed from the EXPLAIN tables, so there's no need to execute the 
         * following, we're leaving it just for the record.

        stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_ACTUALS");
        stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_ARGUMENT");
        stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_DIAGNOSTIC");
        stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_DIAGNOSTIC_DATA");
        stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_OBJECT");
        stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_OPERATOR");
        stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_PREDICATE");
        stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_STATEMENT");
        */

        stmt.close();
    }

    /**
     * Extracts columns that the operator is processing.
     *
     * @param colNamesInExplainStream
     *      column names as they appear in the {@code EXPLAIN_STREAM.COLNAMES} column
     * @param dbo
     *      database object for which the columns correspond (either an {@link Index} or {@link 
     *      Table})
     * @param catalog
     *      to do the binding
     * @return
     *      the columns touched by the operator, as an instance of {@link InterestingOrder}
     * @throws SQLException
     *      if one of the column names can't be bound to the corresponding column
     */
    static InterestingOrder extractColumnsUsedByOperator(
            String colNamesInExplainStream, DatabaseObject dbo, Catalog catalog)
        throws SQLException
    {
        if (colNamesInExplainStream == null || colNamesInExplainStream.trim().equals(""))
            return null;
        // +Q4.PS_SUPPLYCOST(A)+Q4.PS_PARTKEY
        Table table;
        Column col;
        List<Column> columns = new ArrayList<Column>();
        String[] colNamesAndAscending = colNamesInExplainStream.split("\\+");
        String colName;
        Map<Column, Boolean>  ascending = new HashMap<Column, Boolean>();
        boolean asc;
        
        if (dbo instanceof Index) 
            table = ((Index) dbo).getTable();
        else if (dbo instanceof Table)
            table = (Table) dbo;
        else 
            throw new SQLException(" The database object must be either index or table");
        
        for (String tblAndColNameAndAscending : colNamesAndAscending) {
            if (tblAndColNameAndAscending.contains("RID") || tblAndColNameAndAscending.equals(""))
                continue;
            String colNameAndAscending = tblAndColNameAndAscending.split("Q*\\.")[1];
            if (!colNameAndAscending.contains("(")) {
                colName = colNameAndAscending;
                asc = true;
            }
            else {
                colName = colNameAndAscending.split("\\(")[0];
                
                if (colNameAndAscending.split("\\(")[1].contains("A"))
                    asc = true;
                else
                    asc = false;
            }
            
            col = catalog.<Column>findByName(
                    table.getFullyQualifiedName() + "." + colName);
            
            if (col == null)
                throw new RuntimeException("Cannot bind " + colName + " to a column object");

            columns.add(col);
            ascending.put(col, asc);
        }
        
        return new InterestingOrder(columns, ascending);
    }

    /**
     * Extracts the predicates corresponding to each operator from the given result set. It is 
     * assumed that the result set contains the {@code node_id} and {@code predicate_text} columns 
     * and has been previously populated.
     *
     * @param rs
     *      result set containing the list of predicates. More than one entry per operator is 
     *      allowed
     * @return
     *      a mapping where the key is the node_id and the value is a list of predicates used 
     *      internally by the optimizer.
     * @throws SQLException
     *      if an error occurs while reading from the given result set
     */
    static Map<Integer, List<String>> extractOperatorToPredicateListMap(ResultSet rs)
        throws SQLException
    {
        Map<Integer, List<String>> idToPredicateList = new HashMap<Integer, List<String>>();
        List<String> predicatesForOperator;
       
        while (rs.next()) {
            predicatesForOperator = idToPredicateList.get(rs.getInt("node_id"));
            
            if (predicatesForOperator == null) {
                predicatesForOperator = new ArrayList<String>();
                idToPredicateList.put(rs.getInt("node_id"), predicatesForOperator);
            }
            
            if (rs.getString("predicate_text") == null)
                predicatesForOperator.add("");
            else
                predicatesForOperator.add(rs.getString("predicate_text"));
        }

        return idToPredicateList;
    }

    /**
     * Parses the list of predicates for a given operator, binds the contents to the given {@code 
     * catalog} and, for each predicate, it creates an instance of {@link Predicate}.
     *
     * @param predicateList
     *      a list of strings where each corresponds to a record from the {@code 
     *      EXPLAIN_PREDICATE.PREDICATE_TEXT} column
     * @param catalog
     *      the catalog used to retrieve metadata information (to do the binding)
     * @return
     *      a list of predicates for the operator
     * @throws SQLException
     *      if a reference contained in a predicate can't be bound to its metadata counterpart
     */
    static List<Predicate> extractPredicatesUsedByOperator(
            List<String> predicateList, Catalog catalog)
        throws SQLException
    {
        // TODO: bind column to the catalog object later
        List<Predicate> predicates = new ArrayList<Predicate>();        
        if (predicateList.size() == 0)
            return predicates;

        // NOT((Q3.P_TYPE LIKE 'MEDIUM BURNISHED%'))        
        String regex = "[^.Q]+|[Q|[^.Q]+]|[\\.]|[^.Q]+";
        Pattern pattern = Pattern.compile(regex);
        Matcher m;
        String element;
        String predicate;
        boolean isAfterQ;
        for (String strPredicate : predicateList) {
            m = pattern.matcher(strPredicate);
            isAfterQ = false;
            predicate = "";
            while (m.find()) {
                element = strPredicate.substring(m.start(), m.end());
                // Q3.P_BRAND
                if (element.equals("Q")) 
                    isAfterQ = true;
                else {
                    if (!isAfterQ)
                        predicate += element;
                    else if (element.equals(".")) 
                        isAfterQ = false;
                }
            }
            // a selection predicate
            Predicate p = new Predicate(null, predicate);
            predicates.add(p);
        }
        return predicates;
    }

    /**
     * Returns the DBMS-agnostic operator name corresponding to the given DB2-dependent operator 
     * name.
     *
     * @param operatorName
     *      the DB2-dependent operator name
     * @return
     *      one of the DBMS-independent operator names defined in {@link Operator}
     * @see Operator
     */
    static String getOperatorName(String operatorName)
    {
        if (operatorName.equals("IXSCAN"))
            return Operator.INDEX_SCAN;
        else if (operatorName.equals("TBSCAN"))
            return Operator.TABLE_SCAN;
        else if (operatorName.equals("NLJOIN"))
            return Operator.NESTED_LOOP_JOIN;
        else if (operatorName.equals("MSJOIN"))
            return Operator.MERGE_SORT_JOIN;
        else if (operatorName.equals("HSJOIN"))
            return Operator.HASH_JOIN;
        else if (operatorName.equals("FETCH"))
            return Operator.FETCH;
        else if (operatorName.equals("RIDSCN"))
            return Operator.RID_SCAN;
        else
            return operatorName;
    }

    /**
     * Explains the given statement and returns its execution plan.
     *
     *
     * @param connection
     *     connection used to communicate with the DBMS
     * @param sql
     *     statement which the plan is obtained for
     * @param indexes
     *     physical configuration the optimizer should consider when preparing the statement
     * @param catalog
     *      the catalog used to retrieve metadata information (to do the binding)
     * @return
     *     an execution plan
     * @throws SQLException
     *     if something goes wrong while talking to the DBMS
     */
    static SQLStatementPlan getPlan(
            Connection connection, SQLStatement sql, Catalog catalog, Set<Index> indexes)
        throws SQLException
    {
        Statement stmtOperator = connection.createStatement();
        Statement stmtPredicate = connection.createStatement();

        stmtOperator.execute("SET CURRENT EXPLAIN MODE = EVALUATE INDEXES");
        stmtOperator.execute(sql.getSQL());
        stmtOperator.execute("SET CURRENT EXPLAIN MODE = NO");
        
        ResultSet rsOperator = stmtOperator.executeQuery(SELECT_FROM_EXPLAIN);
        ResultSet rsPredicate = stmtPredicate.executeQuery(SELECT_FROM_PREDICATES);
        SQLStatementPlan plan = parsePlan(catalog, rsOperator, rsPredicate, indexes);

        rsOperator.close();
        rsPredicate.close();
        stmtOperator.close();
        stmtPredicate.close();

        return plan;
    }

    /**
     * Returns the update cost of the statement that has been just explained. It assumes that the 
     * type of statement has been checked already, i.e. that the statement is of type {@link 
     * SQLCategory#NOT_SELECT}.
     *
     * @param sqlPlan
     *     plan for the statement
     * @return
     *     the update cost of the plan
     * @throws SQLException
     *      if the plan doesn't contain exactly 3 nodes (i.e. the number of operators in any 
     *      execution plan of an update)
     */
    private static double getUpdateCost(SQLStatementPlan sqlPlan) throws SQLException
    {
        double updateOpCost = -1;
        double childOpCost = -1;

        if (sqlPlan.toList().size() != 3)
            throw new SQLException("Something went wrong for the UPDATE, should have 3 nodes");

        for (Operator o : sqlPlan.toList())
            if (o.getId() == 2)
                updateOpCost = o.getAccumulatedCost();
            else if (o.getId() == 3)
                childOpCost = o.getAccumulatedCost();

        if (updateOpCost == -1 || childOpCost == -1)
            throw new SQLException("Something went wrong for the UPDATE");

        return updateOpCost - childOpCost;
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
    static void insertIntoAdviseIndexTable(Connection connection, Set<Index> configuration)
        throws SQLException
    {
        if (configuration.isEmpty())
            return;

        Statement stmt = connection.createStatement();

        for (Index index : configuration)
            stmt.execute(buildAdviseIndexInsertStatement(index));
        
        stmt.close();
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
     * @param ascending
     *     list being populated with the asc/desc values. The size of the list is equal to the 
     *     {@code columns} list
     * @throws SQLException
     *     if a SQL communication error occurs
     */
    private static void parseColumnNames(
            Table table, String str, List<Column> columns, List<Boolean> ascending)
        throws SQLException
    {
        char c;
        int nameStart;
        boolean newColumn;
        
        c = str.charAt(0);

        if (c == '+')
            ascending.add(true);
        else if (c == '-')
            ascending.add(false);
        else
            throw new SQLException("first character '" + c + "' unexpected in COLNAMES");

        // name starts after +/- symbol
        nameStart = 1;
        
        for (int i = 1; i < str.length(); i++) {
            c = str.charAt(i);

            if (c == '+') {
                ascending.add(true);
                newColumn = true;
            } else if (c == '-') {
                ascending.add(false);
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
     *     result set where operator information is contained. It is assumed that the result set 
     *     contains information for a single statement, and one entry per operator. The columns 
     *     expected in the resultset are {@code operator_name}, {@code object_name}, {@code 
     *     object_name}, {@code cost} and {@code column_names}.
     * @param catalog
     *     used to retrieve metadata information
     * @param predicateList
     *     list of predicates as contained in the {@code SYSTOOLS.EXPLAIN_PREDICATE.PREDICATE_TEXT} 
     *     column. Each predicate contains
     * @param indexes
     *     physical configuration the optimizer should consider when preparing the statement
     * @return
     *     the execution plan
     * @throws SQLException
     *     if an expected column is not in the result set; if a database object referenced by the 
     *     operator can't be found in the catalog.
     */
    static Operator parseNode(
            Catalog catalog, ResultSet rs, List<String> predicateList, Set<Index> indexes)
        throws SQLException
    {
        String name = rs.getString("operator_name");
        String dboSchema = rs.getString("object_schema");
        String dboName = rs.getString("object_name");
        double accomulatedCost = rs.getDouble("cost");
        String columnNames = rs.getString("column_names");
        long cardinality = rs.getLong("cardinality");
        DatabaseObject dbo;
        DatabaseObject columnsFetched;

        Operator op = new Operator(getOperatorName(name.trim()), accomulatedCost, cardinality);

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

        columnsFetched = extractColumnsUsedByOperator(columnNames, dbo, catalog);

        if (columnsFetched != null)
            op.add(columnsFetched);

        op.add(extractPredicatesUsedByOperator(predicateList, catalog));

        return op;
    }

    /**
     * Reads the {@code EXPLAIN_OPERATOR} table and creates a {@link SQLStatementPlan} object out of 
     * it. Assumes that the {@code EXPLAIN_OPERATOR} and {@code EXPLAIN_STREAM} tables contain 
     * entries corresponding to one statement.
     *
     * @param rsOperator
     *     result set object that contains the answer from the DBMS for the query that extracts 
     *     operator information. It is assumed that the result set contains information for a single 
     *     statement, and one entry per operator.
     * @param rsPredicate
     *     result set object that contains the answer from the DBMS for the query that extracts 
     *     predicate information.
     * @param catalog
     *     used to retrieve metadata information
     * @param indexes
     *     physical configuration the optimizer should consider when preparing the statement
     * @return
     *     the execution plan
     * @throws SQLException
     *     if something goes wrong while talking to the DBMS
     * @see #parseNode
     */
    static SQLStatementPlan parsePlan(
            Catalog catalog,
            ResultSet rsOperator,
            ResultSet rsPredicate,
            Set<Index> indexes)
        throws SQLException
    {
        Map<Integer, Operator> idToNode;
        Map<Integer, List<String>> predicateList;
        Operator child;
        Operator parent;
        Operator root;
        SQLStatementPlan plan;
        int operatorId;
        int parentId;

        if (!rsOperator.next())
            throw new SQLException("Empty plan");

        predicateList = extractOperatorToPredicateListMap(rsPredicate);
        root = parseNode(catalog, rsOperator, predicateList.get(1), indexes);
        plan = new SQLStatementPlan(root);
        idToNode = new HashMap<Integer, Operator>();
        
        idToNode.put(1, root);

        while (rsOperator.next()) {
            operatorId = rsOperator.getInt("node_id");
            parentId = rsOperator.getInt("parent_id");
            
            child = parseNode(catalog, rsOperator, predicateList.get(operatorId), indexes);
            parent = idToNode.get(parentId);

            if (parent == null)
                throw new SQLException(child + " expecting parent_id=" + parentId);

            plan.setChild(parent, child);
            idToNode.put(operatorId, child);
        }

        return plan;
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
    private static Set<Index> readAdviseIndexTable(Connection connection, Catalog catalog)
        throws SQLException
    {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM systools.advise_index");
        Set<Index> recommended = new HashSet<Index>();
        List<Column> columns;
        List<Boolean> ascending;
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
            ascending = new ArrayList<Boolean>();
            name = "sat_index_" + new Random().nextInt(Integer.MAX_VALUE);

            parseColumnNames(table, rs.getString("colnames"), columns, ascending);

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

            index = new Index(name, columns, ascending, primary, unique, clustered);

            recommended.add(index);
        }

        rs.close();
        stmt.close();

        return recommended;
    }

    // CHECKSTYLE:OFF
    final static String INSERT_INTO_ADVISE_INDEX_COLUMNS =
        "INSERT INTO systools.advise_index (" +

        // user metadata... extract it from the system's recommended indexes 
        "   EXPLAIN_REQUESTER, " +
        "   TBCREATOR, " +
        
        // table name (string) 
        "   TBNAME, " +

        // '+A-B+C' means "A" ASC, "B" DESC, "C" ASC ...not sure about INCLUDE columns
        "   COLNAMES, " +

        // #Key columns + #Include columns. Must match COLNAMES
        "   COLCOUNT, " +

        // 'P' (primary), 'D' (duplicates allowed), 'U' (unique) 
        "   UNIQUERULE, " +

        // IF unique index THEN #Key columns ELSE -1 
        "   UNIQUE_COLCOUNT, " +

        // 'Y' or 'N' indicating if reverse scans are supported
        "   REVERSE_SCANS, " +

        // 'CLUS', 'REG', 'DIM', 'BLOK' 
        "   INDEXTYPE, " +
        
        // The name of the index and the CREATE INDEX statement (must match)
        "   NAME, " +
        "   CREATION_TEXT, " +
        
        // Indicates if the index is real or hypothetical
        // 'Y' or 'N' 
        "   EXISTS, " +
        
        // Indicates if the index is system defined... should only be true for real indexes
        // 0, 1, or 2 
        "   SYSTEM_REQUIRED, " +
        
        // We use this field to identify an index (also stored locally)
        "   IID, " +
        
        // enable the index for what-if analysis
        // 'Y' or 'N'
        "   USE_INDEX, " +
        
        // statistics, set to -1 to indicate unknown
        "   NLEAF, " +
        "   NLEVELS, " +
        "   FIRSTKEYCARD, " +
        "   FULLKEYCARD, " +
        "   CLUSTERRATIO, " +
        "   AVGPARTITION_CLUSTERRATIO, " +
        "   AVGPARTITION_CLUSTERFACTOR, " +
        "   AVGPARTITION_PAGE_FETCH_PAIRS, " +
        "   DATAPARTITION_CLUSTERFACTOR, " +
        "   CLUSTERFACTOR, " +
        "   SEQUENTIAL_PAGES, " +
        "   DENSITY, " +
        "   FIRST2KEYCARD, " +
        "   FIRST3KEYCARD, " +
        "   FIRST4KEYCARD, " +
        "   PCTFREE, " +

        // empty string instead of -1 for this one
        "   PAGE_FETCH_PAIRS, " +
        // 0 instead of -1 for this one
        "   MINPCTUSED, " +
        
        /* the rest are likely useless */
        "   EXPLAIN_TIME, " +
        "   CREATE_TIME, " +
        "   STATS_TIME, " +
        "   SOURCE_NAME, " +
        "   REMARKS, " +
        "   CREATOR, " +
        "   DEFINER, " +
        "   SOURCE_SCHEMA, " +
        "   SOURCE_VERSION, " +
        "   EXPLAIN_LEVEL, " +
        "   USERDEFINED, " +
        "   STMTNO, " +
        "   SECTNO, " +
        "   QUERYNO, " +
        "   QUERYTAG, " +
        "   PACKED_DESC, " +
        "   RUN_ID, " +
        "   RIDTOBLOCK, " +
        "   CONVERTED) " +
        " VALUES (" +
        "   'DBTune', ";

    final static String INSERT_INTO_ADVISE_INDEX_REST_OF_VALUES =
        "'Y', " +
        "-1, " +
        "-1, " +
        "-1, " +
        "-1, " +
        "-1, " +
        "-1, " +
        "-1, " +
        "'', " +
        "-1, " +
        "-1, " +
        "-1, " +
        "-1, " +
        "-1, " +
        "-1, " +
        "-1, " +
        "-1, " +
        "'', " +
        "0 , " +
        "CURRENT TIMESTAMP, " +
        "CURRENT TIMESTAMP, " +
        "NULL, " +
        "'DBTune', " +
        "'Created by DBTune', " +
        "'SYSTEM', " +
        "'SYSTEM', " +
        "'NULLID', " +
        "'', " +
        "'P', " +
        "1, " +
        "1, " +
        "1, " +
        "1, " +
        "'', " +
        "NULL, " +
        "NULL, " +
        "'N', " +
        "'Z')";

    final static String SELECT_FROM_EXPLAIN =
        "SELECT " +
        "     o.operator_id    AS node_id, " +
        "     s2.target_id     AS parent_id, " +
        "     o.operator_type  AS operator_name, " +
        "     s1.object_schema AS object_schema, " +
        "     s1.object_name   AS object_name," +
        "     s1.stream_count  AS cardinality, " +
        "     o.total_cost     AS cost, " +
        "     cast(cast(s1.column_names AS CLOB(2097152)) AS VARCHAR(255)) " +
        "                      AS column_names " +
        "  FROM " +
        "     systools.explain_operator o " +
        "        LEFT OUTER JOIN " +
        "     systools.explain_stream s2 ON" +
        "           o.operator_id = s2.source_id " +
        "        LEFT OUTER JOIN " +
        "     systools.explain_stream s1 ON " +
        "           o.operator_id = s1.target_id AND " +
        "           o.explain_time = s1.explain_time AND " +
        "           s1.object_name IS NOT NULL " +
        "  WHERE " +
        "     s2.target_id >= 0 OR " +
        "     s2.target_id IS NULL " + // RETURN (the root) has NULL as it's parent
        "  ORDER BY " +
        "     o.operator_id ASC";

    final static String SELECT_FROM_PREDICATES =
        " SELECT " +
        "   o.operator_id   AS node_id, " +
        "   CAST(CAST(p.predicate_text AS CLOB(2097152)) AS VARCHAR(255)) " +
        "                   AS predicate_text " +
        " FROM " +
        "   systools.explain_operator o " +
        "      LEFT OUTER JOIN     " +
        "   systools.explain_predicate p " +
        "         ON o.operator_id = p.operator_id " +             
        " ORDER BY " +
        "          o.operator_id ASC ";

    final static String DELETE_FROM_ADVISE_INDEX = "DELETE FROM SYSTOOLS.ADVISE_INDEX";

    final static String DELETE_FROM_EXPLAIN_INSTANCE = "DELETE FROM SYSTOOLS.EXPLAIN_INSTANCE";
    // CHECKSTYLE:ON

    /**
     * Loads the optimization profiles based on the state of the variables {@link #isFTSDisabled}, 
     * {@link #isNLJDisabled} and the given set of indexes.
     *
     * @param sql
     *     statement which the plan is obtained for
     * @throws SQLException
     *      if the profiles can't be loaded
    private void loadOptimizationProfiles(SQLStatement sql) throws SQLException
    {
        if (isFTSDisabled)
            loadFTSDisabledProfile(sql, connection, ftsDisabledTables);
        //if (isNLJDisabled)
            //loadNLJDisabledProfile(connection, indexes);
    }
     */

    /**
     * @param connection
     *      used to communicate to DB2
     * @throws SQLException
     *      if an error occurs while communicating to the DBMS
    private static void unloadOptimizationProfiles(Connection connection)
        throws SQLException
    {
        Statement stmt = connection.createStatement();
        stmt.execute("SET CURRENT OPTIMIZATION PROFILE = ''");
        stmt.execute("FLUSH OPTIMIZATION PROFILE CACHE");
        stmt.close();
    }
     */

    /**
     * Loads the FTS profile for the given set of tables.
     *
     * @param sql
     *     statement which the plan is obtained for
     * @param connection
     *      used to communicate to DB2
     * @param tables
     *      tables for which the FTS profile is loaded
     * @throws SQLException
     *      if an error occurs while communicating to the DBMS
    private static void loadFTSDisabledProfile(
            SQLStatement sql, Connection connection, Set<Table> tables)
        throws SQLException
    {
        if (tables.size() == 0)
            return;

        if (getSchemas(tables).size() > 1)
            throw new SQLException("Can only apply FTS for tables of the same schema");

        Schema s = null;

        for (Table t : tables) {
            s = t.getSchema();
            break;
        }

        StringBuilder xml = new StringBuilder();

        xml.append(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<OPTPROFILE VERSION=\"9.1.0.0\">\n" +
            "   <STMTPROFILE ID=\"noFTS on given tables foo\">\n" +
            "      <STMTKEY SCHEMA=\"" + s.getName() + "\">\n" +
            "         <![CDATA[" + sql.getSQL() + "]]>\n" +
            "      </STMTKEY>\n" +
            "      <OPTGUIDELINES>\n");

        for (Table t : tables) xml.append(
            "         <IXSCAN TABLE=\"" + t.getName() + "/>");

        xml.append(
            "      </OPTGUIDELINES>\n" +
            "   </STMTPROFILE>\n" +
            "</OPTPROFILE>");

        Statement stmt = connection.createStatement();

        stmt.execute("DELETE FROM systools.opt_profile");

        stmt.execute(
                "INSERT INTO systools.opt_profile values(" +
                "    '" + s.getName() + "', " +
                "    'NOFTS', " +
                "    BLOB('" + xml + "')" +
                ")");

        stmt.execute("SET CURRENT OPTIMIZATION PROFILE = '" + s.getName() + ".NOFTS'");
        stmt.execute("FLUSH OPTIMIZATION PROFILE CACHE");

        stmt.close();
    }
     */
}
