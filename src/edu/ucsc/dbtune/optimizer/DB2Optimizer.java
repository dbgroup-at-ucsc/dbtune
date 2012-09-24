package edu.ucsc.dbtune.optimizer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

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
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Tree.Entry;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Sets.newHashSet;

import static edu.ucsc.dbtune.optimizer.plan.Operator.DELETE;
import static edu.ucsc.dbtune.optimizer.plan.Operator.INDEX_SCAN;
import static edu.ucsc.dbtune.optimizer.plan.Operator.INSERT;
import static edu.ucsc.dbtune.optimizer.plan.Operator.TABLE_SCAN;
import static edu.ucsc.dbtune.optimizer.plan.Operator.TEMPORARY_TABLE_SCAN;
import static edu.ucsc.dbtune.optimizer.plan.Operator.UPDATE;

import static edu.ucsc.dbtune.util.MetadataUtils.find;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesReferencingTable;
import static edu.ucsc.dbtune.util.MetadataUtils.getReferencedSchemas;
import static edu.ucsc.dbtune.util.MetadataUtils.getReferencedTables;

import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;

/**
 * Interface to the DB2 optimizer.
 * <p>
 * This class assumes that the {@code EXPLAIN_*}, {@code ADVISE_*} and {@code
 * OPT_PROFILE} tables have been created in the target DB2 instance.
 * 
 * @see <a href="http://bit.ly/vmHlsj">Explain Tables</a>
 * @see <a href="http://bit.ly/xxPpoz">OPT_PROFILE table</a>
 * 
 * @author Karl Schnaitter
 * @author Huascar Sanchez
 * @author Ivo Jimenez
 * @author Quoc Trung Tran
 */
public class DB2Optimizer extends AbstractOptimizer {
    private Connection connection;

    /**
     * Creates a DB2 optimizer with the given information.
     * 
     * @param connection
     *            a live connection to DB2
     */
    public DB2Optimizer(Connection connection) {
        this.connection = connection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExplainedSQLStatement explain(SQLStatement sql, Set<Index> indexes)
            throws SQLException {
        Set<Index> used;
        Map<Index, Double> updateCostPerIndex;
        SQLStatementPlan plan;
        Table updatedTable;
        double selectCost;
        double baseTableUpdateCost;

        clearAdviseAndExplainTables(connection);

        loadOptimizationProfiles(sql, indexes);

        insertIntoAdviseIndexTable(connection, indexes);

        plan = getPlan(connection, sql, catalog, indexes);
        used = newHashSet(plan.getIndexes());

        plan.setStatement(sql);

        if (sql.getSQLCategory().isSame(SQLCategory.NOT_SELECT)) {
            updatedTable = getUpdatedTable(connection, catalog, plan);
            baseTableUpdateCost = getBaseTableUpdateCost(plan);
            updateCostPerIndex = getUpdatedIndexes(updatedTable,
                    baseTableUpdateCost, indexes);
        } else {
            baseTableUpdateCost = 0.0;
            updatedTable = null;
            updateCostPerIndex = new HashMap<Index, Double>();
        }

        selectCost = plan.getRootOperator().getAccumulatedCost()
                - baseTableUpdateCost;

        unloadOptimizationProfiles();

        whatIfCount++;

        return new ExplainedSQLStatement(sql, plan, this, selectCost,
                updatedTable, baseTableUpdateCost, updateCostPerIndex, indexes,
                used, 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> recommendIndexes(SQLStatement sql) throws SQLException {
        clearAdviseAndExplainTables(connection);

        Statement stmt = connection.createStatement();

        stmt.execute("SET CURRENT EXPLAIN MODE = RECOMMEND INDEXES");
        stmt.execute(sql.getSQL());
        stmt.execute("SET CURRENT EXPLAIN MODE = NO");
        stmt.close();

        Set<Index> recommended = readAdviseIndexTable(connection, catalog);

        clearAdviseAndExplainTables(connection);

        for (Index i : recommended)
            i.setCreationCost(getCreationCost(this, new HashSet<Index>(), i));

        return recommended;
    }

    /**
     * Returns a comma-separated list of the names of columns contained in the
     * given index.
     * 
     * @param index
     *            an index
     * @return a string containing the comma-separated list of column names
     */
    static String buildColumnNamesValue(Index index) {
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
     * Returns a SQL statement that can be executed to insert the given index
     * into the {@code ADVISE_INDEX} table.
     * 
     * @param ps
     *            prepared statement
     * @param index
     *            an index
     * @throws SQLException
     *             if an error occurs while adding the batched insert
     */
    static void addAdviseIndexInsertStatement(PreparedStatement ps, Index index)
            throws SQLException {
        ps.setString(1, index.getTable().getSchema().getName());
        ps.setString(2, index.getTable().getName());
        ps.setString(3, buildColumnNamesValue(index));
        ps.setInt(4, index.size());

        if (index.isPrimary())
            ps.setString(5, "P");
        else if (index.isUnique())
            ps.setString(5, "U");
        else
            ps.setString(5, "D");

        if (index.isUnique())
            ps.setInt(6, index.size());
        else
            ps.setInt(6, -1);

        if (index.isReversible())
            ps.setString(7, "Y");
        else
            ps.setString(7, "N");

        if (index.isClustered())
            ps.setString(8, "CLUS");
        else
            ps.setString(8, "REG");

        ps.setString(9, index.getFullyQualifiedName());
        ps.setString(10, " ");
        ps.setString(11, "N");
        ps.setString(12, "0");
        ps.setInt(13, index.getId());

        ps.addBatch();
    }

    /**
     * Clears the content from the explain tables, so that the information of
     * only one statement is contained in them.
     * 
     * @param connection
     *            connection used to communicate with the DBMS
     * @throws SQLException
     *             if an error occurs while operating over the {@code
     *             ADVISE_INDEX} table
     */
    static void clearAdviseAndExplainTables(Connection connection)
            throws SQLException {
        Statement stmt = connection.createStatement();

        stmt.executeUpdate(DELETE_FROM_ADVISE_INDEX);
        stmt.executeUpdate(DELETE_FROM_EXPLAIN_INSTANCE);

        /*
         * there's a trigger on EXPLAIN_INSTANCE that causes all data
         * corresponding to an explain instance to be removed from the EXPLAIN
         * tables, so there's no need to execute the following, we're leaving it
         * just for the record.
         * 
         * stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_ACTUALS");
         * stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_ARGUMENT");
         * stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_DIAGNOSTIC");
         * stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_DIAGNOSTIC_DATA");
         * stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_OBJECT");
         * stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_OPERATOR");
         * stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_PREDICATE");
         * stmt.executeUpdate("DELETE FROM SYSTOOLS.EXPLAIN_STATEMENT");
         */

        stmt.close();
    }

    /**
     * Extracts columns that the operator is processing.
     * 
     * @param operator
     *            The operator this interesting order belongs to
     * @param colNamesInExplainStream
     *            column names as they appear in the {@code
     *            EXPLAIN_STREAM.COLNAMES} column
     * @param dbo
     *            database object for which the columns correspond (either an
     *            {@link Index} or {@link Table})
     * @param catalog
     *            to do the binding
     * @return the columns touched by the operator, as an instance of
     *         {@link InterestingOrder}
     * @throws SQLException
     *             if one of the column names can't be bound to the
     *             corresponding column
     */
    static InterestingOrder extractColumnsUsedByOperator(Operator operator,
            String colNamesInExplainStream, DatabaseObject dbo, Catalog catalog)
            throws SQLException {
        if (colNamesInExplainStream == null
                || colNamesInExplainStream.trim().equals(""))
            return null;

        // +Q4.PS_SUPPLYCOST(A)+Q4.PS_PARTKEY
        Table table;
        Column col;
        List<Column> columns = new ArrayList<Column>();
        String[] colNamesAndAscending = colNamesInExplainStream.split("\\+");
        String colName;
        Map<Column, Boolean> ascending = new HashMap<Column, Boolean>();
        boolean asc;

        if (dbo instanceof Index)
            table = ((Index) dbo).getTable();
        else if (dbo instanceof Table)
            table = (Table) dbo;
        else
            throw new SQLException(
                    "The database object must be either index or table");

        for (String tblAndColNameAndAscending : colNamesAndAscending) {
            if (tblAndColNameAndAscending.contains("RID")
                    || tblAndColNameAndAscending.equals(""))
                continue;
            // In explain tables, each operator may has an alias, for example,
            // Q1, Q2, ...
            // The following code extract the alias
            String[] qidAndColname = tblAndColNameAndAscending.split("\\.");
            String colNameAndAscending = qidAndColname[1];
            if (operator.aliasInExplainTables != null
                    && !operator.aliasInExplainTables.equals(qidAndColname[0]))
                Rt.error("ERROR");
            operator.aliasInExplainTables = qidAndColname[0];
            if (!colNameAndAscending.contains("(")) {
                colName = colNameAndAscending;
                asc = true;
            } else {
                colName = colNameAndAscending.split("\\(")[0];

                if (colNameAndAscending.split("\\(")[1].contains("A"))
                    asc = true;
                if (colNameAndAscending.split("\\(")[1].contains("D"))
                    asc = false;
                else {
                    asc = false;
                    // System.out.println("Unknown order for " + colName);
                }
            }

            col = catalog.<Column> findByName(table.getFullyQualifiedName()
                    + "." + colName);

            if (col == null)
                throw new RuntimeException("Cannot bind " + table.getFullyQualifiedName()
                        + "." + colName
                        + " to a column object");

            columns.add(col);
            ascending.put(col, asc);
        }

        if (columns.size() == 0)
            return null;

        return new InterestingOrder(columns, ascending);
    }

    /**
     * Extracts the predicates corresponding to each operator from the given
     * result set. It is assumed that the result set contains the {@code
     * node_id} and {@code predicate_text} columns and has been previously
     * populated.
     * 
     * @param rs
     *            result set containing the list of predicates. More than one
     *            entry per operator is allowed
     * @return a mapping where the key is the node_id and the value is a list of
     *         predicates used internally by the optimizer.
     * @throws SQLException
     *             if an error occurs while reading from the given result set
     */
    static Map<Integer, List<String>> extractOperatorToPredicateListMap(
            ResultSet rs) throws SQLException {
        Map<Integer, List<String>> idToPredicateList = new HashMap<Integer, List<String>>();
        List<String> predicatesForOperator;

        while (rs.next()) {
            predicatesForOperator = idToPredicateList.get(rs.getInt("node_id"));

            if (predicatesForOperator == null) {
                predicatesForOperator = new ArrayList<String>();
                idToPredicateList.put(rs.getInt("node_id"),
                        predicatesForOperator);
            }

            if (rs.getString("predicate_text") == null)
                predicatesForOperator.add("");
            else
                predicatesForOperator.add(rs.getString("predicate_text"));
        }

        return idToPredicateList;
    }

    /**
     * Parses the list of predicates for a given operator, binds the contents to
     * the given {@code catalog} and, for each predicate, it creates an instance
     * of {@link Predicate}.
     * 
     * @param predicateList
     *            a list of strings where each corresponds to a record from the
     *            {@code EXPLAIN_PREDICATE.PREDICATE_TEXT} column
     * @param catalog
     *            the catalog used to retrieve metadata information (to do the
     *            binding)
     * @return a list of predicates for the operator
     * @throws SQLException
     *             if a reference contained in a predicate can't be bound to its
     *             metadata counterpart
     */
    static List<Predicate> extractPredicatesUsedByOperator(
            List<String> predicateList, Catalog catalog) throws SQLException {
        List<Predicate> predicates = new ArrayList<Predicate>();

        if (predicateList.size() == 0)
            return predicates;

        Pattern tableReferencePattern = Pattern.compile("Q\\d+\\.");

        for (String predicate : predicateList) {

            String strPredicate = predicate;

            if (strPredicate == null || strPredicate.trim().equals(""))
                continue;

            if (strPredicate.contains("SELECT")) {
                // predicate contains a subquery; we need to replace $RID$
                // references, eg.
                //
                // EXISTS(SELECT $RID$ FROM t AS q1 WHERE q1.val1 < q1.val2)
                strPredicate = strPredicate.replaceAll("\\$RID\\$", "RID()");
            }

            final Matcher matcher = tableReferencePattern.matcher(strPredicate);

            if (!matcher.find()) {
                // no table references (eg. 3 = 3), just create the predicate
                predicates.add(new Predicate(null, strPredicate));
                continue;
            }

            final String firstTableReference = matcher.group();

            if (!matcher.find()) {
                // only one table references (eg. Q1.L_QUANTITY < 3); create the
                // predicate without
                predicates.add(new Predicate(null, strPredicate.replaceAll(
                        "Q\\d+\\.", "")));
                continue;
            }

            final String secondTableReference = matcher.group();

            if (firstTableReference.equals(secondTableReference)) {
                // we found something like Q1.L_QUANTITY = Q1.L_PRICE or a
                // multi-predicate, which
                // sometimes includes SELECTIVITY hints, eg.:
                // (((Q7.SS_NET_PROFIT >= 0 SELECTIVITY 1.000000) AND
                // (Q7.SS_NET_PROFIT <= 2000
                // SELECTIVITY 1.000000) SELECTIVITY 1.000000) OR SELECTIVITY
                // 1.000000)
                predicates.add(new Predicate(null, strPredicate.replaceAll(
                        "Q\\d+\\.", "").replaceAll(
                        "SELECTIVITY \\d*\\.\\d*\\)", "\\)")));
            }

            // ignore since we have found a join predicate and we only care
            // about predicates that
            // reference only the relation in question
        }

        return predicates;
    }

    /**
     * Returns the DBMS-agnostic operator name corresponding to the given
     * DB2-dependent operator name.
     * 
     * @param operatorName
     *            the DB2-dependent operator name
     * @return one of the DBMS-independent operator names defined in
     *         {@link Operator}
     * @see Operator
     */
    static String getOperatorName(String operatorName) {
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
        else if (operatorName.equals("IXAND"))
            return Operator.INDEX_AND;
        else if (operatorName.equals("IXOR"))
            return Operator.INDEX_OR;
        else if (operatorName.equals("TEMP"))
            return Operator.TEMPORARY_TABLE_SCAN;
        else
            return operatorName;
    }

    /**
     * Explains the given statement and returns its execution plan.
     * 
     * 
     * @param connection
     *            connection used to communicate with the DBMS
     * @param sql
     *            statement which the plan is obtained for
     * @param indexes
     *            physical configuration the optimizer should consider when
     *            preparing the statement
     * @param catalog
     *            the catalog used to retrieve metadata information (to do the
     *            binding)
     * @return an execution plan
     * @throws SQLException
     *             if something goes wrong while talking to the DBMS
     */
    static SQLStatementPlan getPlan(Connection connection, SQLStatement sql,
            Catalog catalog, Set<Index> indexes) throws SQLException {
        if (true)
            return new ExplainTables().getPlan(connection, sql, catalog,
                    indexes);
        Statement stmt = connection.createStatement();

        stmt.execute("SET CURRENT EXPLAIN MODE = EVALUATE INDEXES");
        stmt.execute(sql.getSQL());
        stmt.execute("SET CURRENT EXPLAIN MODE = NO");
        stmt.close();
        Statement stmtOperator = connection.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        Statement stmtPredicate = connection.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        ResultSet rsOperator = stmtOperator.executeQuery(SELECT_FROM_EXPLAIN);
        ResultSet rsPredicate = stmtPredicate
                .executeQuery(SELECT_FROM_PREDICATES);
        SQLStatementPlan plan = parsePlan(catalog, rsOperator, rsPredicate,
                indexes);

        rsOperator.close();
        rsPredicate.close();
        stmtOperator.close();
        stmtPredicate.close();

        return plan;
    }

    /**
     * Returns the update cost of the statement that has been just explained. It
     * assumes that the type of statement has been checked already, i.e. that
     * the statement is of type {@link SQLCategory#NOT_SELECT}.
     * 
     * @param sqlPlan
     *            plan for the statement
     * @return the update cost of the plan
     * @throws SQLException
     *             if the plan doesn't contain exactly 3 nodes (i.e. the number
     *             of operators in any execution plan of an update)
     */
    private static double getBaseTableUpdateCost(SQLStatementPlan sqlPlan)
            throws SQLException {
        if (sqlPlan.size() < 3)
            throw new SQLException(
                    "NOT_SELECT plan should have at least 3 nodes but has "
                            + sqlPlan.size());

        Operator root = sqlPlan.getRootOperator();

        if (sqlPlan.getChildren(root).size() != 1)
            throw new SQLException("Root should have only one child");

        Operator update = sqlPlan.getChildren(root).get(0);

        if (!update.getName().equals(UPDATE)
                && !update.getName().equals(INSERT)
                && !update.getName().equals(DELETE))
            throw new SQLException("Child of root should be " + UPDATE + ", "
                    + INSERT + " or " + DELETE);

        if (sqlPlan.getChildren(update).size() != 1)
            throw new SQLException("Write operator should have only one child");

        Operator select = sqlPlan.getChildren(update).get(0);

        return update.getAccumulatedCost() - select.getAccumulatedCost();
    }

    /**
     * Obtains the update cost for each index, which corresponds to the cost of
     * the base table being updated.
     * 
     * @param updatedTable
     *            table that is updated
     * @param baseTableUpdateCost
     *            cost of doing the update over the base table
     * @param indexes
     *            set of indexes considered in the what-if call
     * @return map of costs for each updated index
     */
    private static Map<Index, Double> getUpdatedIndexes(Table updatedTable,
            double baseTableUpdateCost, Set<Index> indexes) {
        Map<Index, Double> updateCostPerIndex = new HashMap<Index, Double>();

        for (Index i : getIndexesReferencingTable(indexes, updatedTable))
            updateCostPerIndex.put(i, baseTableUpdateCost);

        return updateCostPerIndex;
    }

    /**
     * Extracts the table that is being updated by the plan.
     * 
     * @param connection
     *            connection used to communicate with the DBMS
     * @param catalog
     *            to do the binding
     * @param sqlPlan
     *            the plan
     * @return the updated table
     * @throws SQLException
     *             if the plan doesn't contain exactly 3 nodes (i.e. the number
     *             of operators in any execution plan of an update)
     */
    private static Table getUpdatedTable(Connection connection,
            Catalog catalog, SQLStatementPlan sqlPlan) throws SQLException {
        if (!sqlPlan.contains(UPDATE) && !sqlPlan.contains(INSERT)
                && !sqlPlan.contains(DELETE))
            throw new SQLException("Plan should contain a " + NOT_SELECT
                    + " operator");

        // For the particular case of updates, the EXPLAIN_STREAM doesn't have
        // the parent-child
        // relationship as the {@link SELECT_FROM_EXPLAIN} query expect it, so
        // we have to execute
        // another query to identify the table that is associated to the UPDATE
        // operator
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(SELECT_FROM_EXPLAIN_FOR_UPDATE);

        if (!rs.next())
            throw new SQLException(
                    "No output for SELECT_FROM_EXPLAIN_FOR_UPDATE query");

        String dboSchema = rs.getString("object_schema").trim();
        String dboName = rs.getString("object_name").trim();

        if (rs.next())
            throw new SQLException(
                    "SELECT_FROM_EXPLAIN_FOR_UPDATE should reference one database object only");

        rs.close();
        stmt.close();

        DatabaseObject dbo = extractDatabaseObjectReferenced(catalog,
                new HashSet<Index>(), dboSchema, dboName);

        if (!(dbo instanceof Table))
            throw new SQLException("Object associated with " + NOT_SELECT
                    + " should be a table");

        boolean isAssigned = false;

        for (Operator o : sqlPlan.toList())
            if (o.getName().equals(UPDATE) || o.getName().equals(INSERT)
                    || o.getName().equals(DELETE)) {
                isAssigned = true;
                sqlPlan.assignDatabaseObject(o, dbo);
            }

        if (!isAssigned)
            throw new SQLException("Can't find " + NOT_SELECT
                    + " operator to associate with " + dbo);

        return (Table) dbo;
    }

    /**
     * Loads the given configuration the {@code ADVISE_INDEX} table.
     * 
     * @param configuration
     *            configuration being created
     * @param connection
     *            connection used to communicate with the DBMS
     * @throws SQLException
     *             if an error occurs while operating over the {@code
     *             ADVISE_INDEX} table
     */
    static void insertIntoAdviseIndexTable(Connection connection,
            Set<Index> configuration) throws SQLException {
        if (configuration.isEmpty())
            return;

        PreparedStatement ps = connection
                .prepareStatement(INSERT_INTO_ADVISE_INDEX);

        for (Index index : configuration)
            if (index.size() > 0)
                addAdviseIndexInsertStatement(ps, index);

        ps.executeBatch();
        ps.clearBatch();
        ps.close();
    }

    /**
     * Loads the optimization profiles based on the state of the variables
     * {@link #isFTSDisabled}, {@link #isNLJDisabled} and the given set of
     * indexes.
     * 
     * @param sql
     *            statement for which the plan is being obtained
     * @param indexes
     *            tables referenced by these set are included in the
     *            optimization profile so that NO-FTS restriction is set
     * @throws SQLException
     *             if the profiles can't be loaded; if the indexes refer to
     *             tables from more than one schema
     */
    private void loadOptimizationProfiles(SQLStatement sql, Set<Index> indexes)
            throws SQLException {
        if (isFTSDisabled)
            loadFTSDisabledProfile(sql.getSQL().replaceAll("'", "''"),
                    connection, getReferencedTables(indexes));
    }

    /**
     * Unloads the optimization profiles.
     * 
     * @throws SQLException
     *             if {@link #unloadOptimizationProfiles(Connnection)} throws an
     *             exception
     */
    private void unloadOptimizationProfiles() throws SQLException {
        unloadOptimizationProfiles(connection);
    }

    /**
     * @param connection
     *            used to communicate to DB2
     * @throws SQLException
     *             if an error occurs while communicating to the DBMS
     */
    private static void unloadOptimizationProfiles(Connection connection)
            throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("SET CURRENT OPTIMIZATION PROFILE = ''");
        stmt.execute("FLUSH OPTIMIZATION PROFILE CACHE");
        stmt.close();
    }

    /**
     * Loads the FTS profile for the given set of tables.
     * 
     * @param sql
     *            statement which the plan is obtained for
     * @param connection
     *            used to communicate to DB2
     * @param tables
     *            tables for which the FTS profile is loaded
     * @throws SQLException
     *             if an error occurs while communicating to the DBMS
     */
    private static void loadFTSDisabledProfile(String sql,
            Connection connection, Set<Table> tables) throws SQLException {
        if (tables.size() == 0)
            return;

        Set<Schema> referencedSchemas = getReferencedSchemas(tables);

        if (referencedSchemas.size() > 1)
            throw new SQLException(
                    "Can only apply optimization profiles on ONE schema");

        Schema s = get(referencedSchemas, 0);

        StringBuilder xml = new StringBuilder();

        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<OPTPROFILE VERSION=\"9.1.0.0\">\n"
                + "   <STMTPROFILE ID=\"no FTS\">\n"
                + "      <STMTKEY SCHEMA=\"" + s.getName() + "\">\n"
                + "         <![CDATA[" + sql + "]]>\n" + "      </STMTKEY>\n"
                + "      <OPTGUIDELINES>\n");
        for (Table t : tables)
            xml.append("<IXSCAN TABLE=\"" + t.getName() + "\"/>\n").append(
                    "<REOPT VALUE=\"NONE\"/>\n");
        xml.append("      </OPTGUIDELINES>\n" + "   </STMTPROFILE>\n"
                + "</OPTPROFILE>");

        Statement stmt = connection.createStatement();

        stmt.execute("DELETE FROM systools.opt_profile");

        stmt.execute("INSERT INTO systools.opt_profile VALUES(" + "    '"
                + s.getName() + "', " + "    'NOFTS', " + "    BLOB('" + xml
                + "')" + ")");

        stmt.execute("SET CURRENT OPTIMIZATION PROFILE = '" + s.getName()
                + ".NOFTS'");
        stmt.execute("FLUSH OPTIMIZATION PROFILE CACHE");
        stmt.execute("SET CURRENT SCHEMA=" + s.getName());

        stmt.close();
    }

    /**
     * Parses the {@code ADVISE_INDEX.COLNAMES} column in order to extract the
     * list of referenced columns and their corresponding ASC/DESC values. The
     * read values are placed in the {@code columns} and {@code descending}
     * lists sent as arguments.
     * 
     * @param table
     *            table from which the columns belong to
     * @param str
     *            string being parsed
     * @param columns
     *            list being populated with the name of columns
     * @param ascending
     *            list being populated with the asc/desc values. The size of the
     *            list is equal to the {@code columns} list
     * @throws SQLException
     *             if a SQL communication error occurs
     */
    private static void parseColumnNames(Table table, String str,
            List<Column> columns, List<Boolean> ascending) throws SQLException {
        char c;
        int nameStart;
        boolean newColumn;

        c = str.charAt(0);

        if (c == '+')
            ascending.add(true);
        else if (c == '-')
            ascending.add(false);
        else
            throw new SQLException("first character '" + c
                    + "' unexpected in COLNAMES");

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
                    throw new SQLException(
                            "empty column name found in ADVISE_INDEX.COLNAMES");
                columns.add(table.findColumn(str.substring(nameStart, i)));
                nameStart = i + 1;
            }
        }

        if (str.length() - nameStart < 1)
            throw new SQLException(
                    "empty column name found in ADVISE_INDEX.COLNAMES");

        columns.add(table.findColumn(str.substring(nameStart, str.length())));
    }

    /**
     * Reads an entry in the given result set object and creates a
     * {@link Operator} object out of it.
     * 
     * @param rs
     *            result set where operator information is contained. It is
     *            assumed that the result set contains information for a single
     *            statement, and one entry per operator. The columns expected in
     *            the resultset are {@code operator_name}, {@code object_name},
     *            {@code object_name}, {@code cost} and {@code column_names}.
     * @param catalog
     *            used to retrieve metadata information
     * @param predicateList
     *            list of predicates as contained in the {@code
     *            SYSTOOLS.EXPLAIN_PREDICATE.PREDICATE_TEXT} column. Each
     *            predicate contains
     * @param indexes
     *            physical configuration the optimizer should consider when
     *            preparing the statement
     * @return the execution plan
     * @throws SQLException
     *             if an expected column is not in the result set; if a database
     *             object referenced by the operator can't be found in the
     *             catalog.
     */
    static Operator parseNode(Catalog catalog, ResultSet rs,
            List<String> predicateList, Set<Index> indexes) throws SQLException {
        int id = rs.getInt("operator_id");
        String name = rs.getString("operator_name");
        String dboSchema = rs.getString("object_schema");
        String dboName = rs.getString("object_name");
        double accomulatedCost = rs.getDouble("cost");
        String columnNames = rs.getString("column_names");
        double cpuCost = rs.getDouble("cpucost");
        double ioCost = rs.getDouble("iocost");
        long cardinality = rs.getLong("cardinality");
        double first_row_cost = rs.getDouble("FIRST_ROW_COST");
        double re_total_cost = rs.getDouble("RE_TOTAL_COST");
        double re_io_cost = rs.getDouble("RE_IO_COST");
        double re_cpu_cost = rs.getDouble("RE_CPU_COST");
        double buffers = rs.getDouble("BUFFERS");
        DatabaseObject dbo;
        InterestingOrder columnsFetched;

        Operator op = new Operator(getOperatorName(name.trim()),
                accomulatedCost, cardinality);
        op.id = id;
        op.ioCost = ioCost;
        op.cpuCost = cpuCost;
        op.first_row_cost = first_row_cost;
        op.re_total_cost = re_total_cost;
        op.re_io_cost = re_io_cost;
        op.re_cpu_cost = re_cpu_cost;
        op.buffers = buffers;
        op.rawColumnNames = columnNames;
        op.rawPredicateList = predicateList;

        if (dboSchema == null || dboName == null)
            return op;

        dboSchema = dboSchema.trim();
        dboName = dboName.trim();

        if (dboSchema.equalsIgnoreCase("SYSIBM")
                && dboName.equalsIgnoreCase("GENROW")) {
            op = new Operator(TEMPORARY_TABLE_SCAN, accomulatedCost,
                    cardinality);
            op.id = id;
            op.ioCost = ioCost;
            op.cpuCost = cpuCost;
            op.first_row_cost = first_row_cost;
            op.re_total_cost = re_total_cost;
            op.re_io_cost = re_io_cost;
            op.re_cpu_cost = re_cpu_cost;
            op.buffers = buffers;
            op.rawColumnNames = columnNames;
            op.rawPredicateList = predicateList;
            if (columnNames != null && columnNames.length() > 0) {
                Pattern tableReferencePattern = Pattern.compile("Q\\d+\\.");
                Matcher matcher = tableReferencePattern.matcher(columnNames);
                if (matcher.find()) {
                    op.aliasInExplainTables = columnNames.substring(matcher
                            .start(), matcher.end() - 1);
                }
            }
            return op;
        }

        dbo = extractDatabaseObjectReferenced(catalog, indexes, dboSchema,
                dboName);

        if (op.getName().equals(INDEX_SCAN) && !(dbo instanceof Index))
            throw new SQLException("Object for " + INDEX_SCAN
                    + " should be of type Index");
        if (op.getName().equals(TABLE_SCAN) && !(dbo instanceof Table))
            throw new SQLException("Object for " + TABLE_SCAN
                    + " should be of type Table");

        op.add(dbo);

        columnsFetched = extractColumnsUsedByOperator(op, columnNames, dbo,
                catalog);

        if (columnsFetched != null)
            op.addColumnsFetched(columnsFetched);

        op.add(extractPredicatesUsedByOperator(predicateList, catalog));

        return op;
    }

    /**
     * Extracts the database object referenced by an operator.
     * 
     * @param catalog
     *            used to retrieve metadata
     * @param indexes
     *            indexes sent for the what-if call
     * @param dboSchema
     *            schema
     * @param dboName
     *            name of the object referenced
     * @return the corresponding object
     * @throws SQLException
     *             if the object can't be found
     */
    static DatabaseObject extractDatabaseObjectReferenced(Catalog catalog,
            Set<Index> indexes, String dboSchema, String dboName)
            throws SQLException {
        DatabaseObject dbo;

        Schema schema = catalog.findSchema(dboSchema);

        if (schema == null && dboSchema.equalsIgnoreCase("SYSTEM"))
            // SYSTEM means that it's in the ADVISE_INDEX table; at least that's
            // what we can infer
            dbo = catalog.findByQualifiedName(dboName);
        else if (schema != null)
            dbo = schema.findByQualifiedName(dboName);
        else
            throw new SQLException("Impossible to look for " + dboSchema + "."
                    + dboName);

        if (dbo == null)
            // try to find it in the set of indexes passed in the what-if
            // invocation
            dbo = find(indexes, dboName);

        if (dbo == null)
            throw new SQLException("Can't find object with schema " + dboSchema
                    + " and name " + dboName);

        return dbo;
    }

    /**
     * Reads the {@code EXPLAIN_OPERATOR} table and creates a
     * {@link SQLStatementPlan} object out of it. Assumes that the {@code
     * EXPLAIN_OPERATOR} and {@code EXPLAIN_STREAM} tables contain entries
     * corresponding to one statement.
     * 
     * @param rsOperator
     *            result set object that contains the answer from the DBMS for
     *            the query that extracts operator information. It is assumed
     *            that the result set contains information for a single
     *            statement, and one entry per operator.
     * @param rsPredicate
     *            result set object that contains the answer from the DBMS for
     *            the query that extracts predicate information.
     * @param catalog
     *            used to retrieve metadata information
     * @param indexes
     *            physical configuration the optimizer should consider when
     *            preparing the statement
     * @return the execution plan
     * @throws SQLException
     *             if {@code rsOperator} isn't scrollable; if {@code rsOperator}
     *             contains no results; if the plan refers to the same table
     *             more than once.
     * @see #parseNode
     */
    static SQLStatementPlan parsePlan(Catalog catalog, ResultSet rsOperator,
            ResultSet rsPredicate, Set<Index> indexes) throws SQLException {
        Map<Integer, Operator> idToNode;
        Map<Integer, Integer> childToParent;
        Map<Integer, List<String>> predicateList;
        Operator root;
        Operator node;
        SQLStatementPlan plan;
        int operatorId;
        int parentId;
        int nodeCount;

        if (!rsOperator.next())
            throw new SQLException("Empty plan");

        predicateList = extractOperatorToPredicateListMap(rsPredicate);
        root = parseNode(catalog, rsOperator, predicateList.get(1), indexes);
        plan = new SQLStatementPlan(root);
        idToNode = new TreeMap<Integer, Operator>();
        Vector<Integer> ids = new Vector<Integer>();
        childToParent = new HashMap<Integer, Integer>();

        idToNode.put(1, root);

        nodeCount = 0;

        if (!rsOperator.last())
            throw new SQLException("Can't move till the end of the ResultSet");

        nodeCount = rsOperator.getRow();

        rsOperator.beforeFirst();

        while (rsOperator.next()) {
            operatorId = rsOperator.getInt("node_id");
            parentId = rsOperator.getInt("parent_id");

            node = parseNode(catalog, rsOperator,
                    predicateList.get(operatorId), indexes);

            /*
             * Sometimes the same node is used in many places in the tree. For
             * example:
             * 
             * 315.002 ^MSJOIN ( 4) 975904 556122 /--+--\ 19213.1 0.0163952
             * TBSCAN FILTER ( 5) ( 62) 484914 484914 278061 278061 | | 480327
             * 19213.1 TEMP TBSCAN ( 6) ( 63) 471362 484914 271199 278061 | |
             * 480327 480327 GRPBY TEMP ( 7) ( 6) 419316 471362 264337 271199 |
             * 480327 TBSCAN ( 8) 419292 264337
             * 
             * Note how node TEMP (id=6) is used two times. This will cause
             * conflicts since we'll be adding the same node in the tree twice
             * and with different parents. So we have to create a new node with
             * a new ID.
             */
            if (childToParent.get(operatorId) != null) {
                nodeCount++;

                // we add the nodeCount to the main members of the new operator
                // since, internally,
                // the SQLStatementPlan hashes the operator's members, thus if
                // we don't modify them,
                // the new operator will replace the other one. In terms of the
                // example, even though
                // we're creating a new node for TEMP (either of the two), by
                // not modifying it's
                // members we're in practice leaving only one
                node = new Operator(node.getName(), node.getAccumulatedCost()
                        + nodeCount, node.getCardinality() + nodeCount, node
                        .getDatabaseObjects(), node.getPredicates(), node
                        .getColumnsFetched());
                operatorId = nodeCount;
            }

            childToParent.put(operatorId, parentId);
            idToNode.put(operatorId, node);
            ids.add(operatorId);
        }

        // make sure parent appear first
        Collections.sort(ids, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1 - o2;
            }
        });
        for (Integer k : ids) {
            setChild(k, idToNode, childToParent, plan);
        }

        calculateOperatorInternalCost(plan, plan.getRootOperator(),1);

        return plan;
    }

    /**
     * Insert a child node into the tree. If parent node doesn't exist, insert
     * it first.
     * 
     * @param k
     * @param idToNode
     * @param childToParent
     * @param plan
     * @throws SQLException
     */
    private static void setChild(int k, Map<Integer, Operator> idToNode,
            Map<Integer, Integer> childToParent, SQLStatementPlan plan)
            throws SQLException {
        if (k == 1)
            // ignore the root
            return;
        Operator child;
        Operator parent;

        child = idToNode.get(k);
        int parentId = childToParent.get(k);
        parent = idToNode.get(parentId);

        if (parent == null)
            throw new SQLException("Inserting " + child + " expecting parent "
                    + childToParent.get(k));

        if (plan.elements.get(child) != null)
            return;
        if (plan.elements.get(parent) == null)
            setChild(parentId, idToNode, childToParent, plan);
        try {
            plan.setChild(parent, child);
        } catch (IllegalArgumentException ex) {
            throw new SQLException(
                    "Duplicate operator found; is statement querying the same table more than "
                            + "once? The DBTune API doesn't handle this case yet",
                    ex);
        } catch (NoSuchElementException ex) {
            Rt.p(parent.id + " " + child.id);
            throw new SQLException("Can't find parent of " + child.getName()
                    + ": " + parent, ex);
        }
    }

    /**
     * calculate internal cost of each operator
     */
    static void calculateOperatorInternalCost(SQLStatementPlan plan,
            Operator node, double coeff) {
        node.coefficient = coeff;
        List<Operator> children = plan.getChildren(node);
        if (children.size() == 0 && node.getDatabaseObjects().size()>0)
            return;
        if (node.getName().equals(Operator.NLJ)) {
            if (children.size() != 2)
                throw new Error("NLJ operator has " + children.size()
                        + " children");
            Operator left = children.get(0);
            Operator right = children.get(1);
            if ("INNER".equals(left.joinInput) && "OUTER".equals(right.joinInput)) {
                List<Entry<Operator>> list = plan.getChildrenForSwap(node);
                Entry<Operator> a0 = list.remove(0);
                Entry<Operator> a1 = list.remove(0);
                list.add(0, a1);
                list.add(1, a0);
                children = plan.getChildren(node);
                left = children.get(0);
                right = children.get(1);
            }
            if (!("OUTER".equals(left.joinInput) && "INNER".equals(right.joinInput))) {
                Rt.p(left.joinInput);
                Rt.p(right.joinInput);
                throw new Error();
            }

            node.internalCost = 0;
            left.cardinalityNLJ = (node.accumulatedCost - left.accumulatedCost)
                    / right.accumulatedCost;
            if (left.cardinalityNLJ < 1)
                left.cardinalityNLJ = 1;
            calculateOperatorInternalCost(plan, left, coeff);
            calculateOperatorInternalCost(plan, right, coeff
                    * left.cardinalityNLJ);
        } else {
            node.internalCost = node.accumulatedCost;
            for (Operator operator : children) {
                node.internalCost -= operator.accumulatedCost;
            }
            for (Operator operator : children) {
                calculateOperatorInternalCost(plan, operator, coeff);
            }
        }
    }

    /**
     * Reads the content of the {@code ADVISE_INDEX}, after a {@code RECOMMEND
     * INDEXES} operation has been done. Creates one index per each record in
     * the table.
     * 
     * @param connection
     *            connection used to communicate with the DBMS
     * @param catalog
     *            used to retrieve metadata information
     * @param indexBytes
     *            the mapping between index's name in ADVISE_INDEX table
     *            to its size     
     * @return the cost of the plan
     * @throws SQLException
     *             if something goes wrong while talking to the DBMS
     */
    public static Set<Index> readAdviseIndexTable(Connection connection,
            Catalog catalog, Map<String, Long> indexBytes) throws SQLException 
    {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM systools.advise_index");
        Set<Index> recommended = new HashSet<Index>();
        List<Column> columns;
        List<Boolean> ascending;
        Schema schema;
        Table table;
        Index index;
        boolean unique = false;
        boolean clustered = false;
        boolean primary = false;
        int pageSize = -1;

        while (rs.next()) {
            if (pageSize == -1)
                pageSize = getPageSizeForTableSpaceReferencedInAdviseIndexTable(connection);

            schema = catalog.findSchema(rs.getString("tbcreator").trim());
            table = schema.findTable(rs.getString("tbname").trim());
            columns = new ArrayList<Column>();
            ascending = new ArrayList<Boolean>();

            parseColumnNames(table, rs.getString("colnames"), columns,
                    ascending);

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

            index = new Index(columns, ascending, primary, unique, clustered);

            if (rs.getString("reverse_scans").trim().equals("Y"))
                index.setScanOption(Index.REVERSIBLE);
            else
                index.setScanOption(Index.NON_REVERSIBLE);
            
            String nameInAdviseTable = rs.getString("NAME");
            if (!indexBytes.containsKey(nameInAdviseTable))
                throw new RuntimeException(" No size information for index: " + nameInAdviseTable);

            index.setBytes((long) indexBytes.get(nameInAdviseTable));            
            index.setName(index.getName());
            recommended.add(index);
        }

        rs.close();
        stmt.close();

        return recommended;
    }
    
    /**
     * Reads the content of the {@code ADVISE_INDEX}, after a {@code RECOMMEND
     * INDEXES} operation has been done. Creates one index per each record in
     * the table.
     * 
     * @param connection
     *            connection used to communicate with the DBMS
     * @param catalog
     *            used to retrieve metadata information
     * @return the cost of the plan
     * @throws SQLException
     *             if something goes wrong while talking to the DBMS
     */
    public static Set<Index> readAdviseIndexTable(Connection connection,
            Catalog catalog) throws SQLException 
    {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM systools.advise_index");
        Set<Index> recommended = new HashSet<Index>();
        List<Column> columns;
        List<Boolean> ascending;
        Schema schema;
        Table table;
        Index index;
        boolean unique = false;
        boolean clustered = false;
        boolean primary = false;
        int pageSize = -1;

        while (rs.next()) {
            if (pageSize == -1)
                pageSize = getPageSizeForTableSpaceReferencedInAdviseIndexTable(connection);

            schema = catalog.findSchema(rs.getString("tbcreator").trim());
            table = schema.findTable(rs.getString("tbname").trim());
            columns = new ArrayList<Column>();
            ascending = new ArrayList<Boolean>();

            parseColumnNames(table, rs.getString("colnames"), columns,
                    ascending);

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

            index = new Index(columns, ascending, primary, unique, clustered);

            if (rs.getString("reverse_scans").trim().equals("Y"))
                index.setScanOption(Index.REVERSIBLE);
            else
                index.setScanOption(Index.NON_REVERSIBLE);
            
            index.setBytes((long) rs.getInt("nleaf") * (long) pageSize);
            // XXX: a more sophisticated way of calculating size:
            // http://ibm.co/xsH4QC
            index.setName(index.getName());
            recommended.add(index);
        }

        rs.close();
        stmt.close();

        return recommended;
    }

    /**
     * Returns the index creation cost for a given index.
     * 
     * @param optimizer
     *            used to execute what-if calls
     * @param indexes
     *            set of indexes that are assumed to exist when the index is
     *            created
     * @param index
     *            index for which the creation cost is retrieved
     * @return the creation cost
     * @throws SQLException
     *             if something goes wrong while doing the what-if call
     */
    public static double getCreationCost(Optimizer optimizer,
            Set<Index> indexes, Index index) throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT *");
        sb.append("  FROM ").append(index.getTable().getFullyQualifiedName());
        sb.append("  ORDER BY ");

        for (Column c : index.columns())
            sb.append(c.getName()).append(
                    index.isAscending(c) ? " ASC, " : " DESC, ");

        sb.delete(sb.length() - 2, sb.length() - 1);

//        if ("SELECT *  FROM NREF.PROTEIN  ORDER BY NREF_ID ASC".equals(sb.toString().trim())) {
//            Rt.p(sb.toString());
//            Rt.p(indexes);
//            ExplainTables.dump=true;
//            }
        return optimizer.explain(sb.toString(), indexes).getSelectCost();
    }

    /**
     * Returns the page size (in bytes) of the table space referenced in the
     * {@code ADVISE_INDEX} table.
     * 
     * @param connection
     *            connection used to communicate with the DBMS
     * @return the page size for the tablespace
     * @throws SQLException
     *             if more than one table space is referenced in the {@code
     *             ADVISE_INDEX} table and none of them have the same page size
     *             assigned to them; if a JDBC error occurs while communicating
     *             to the DBMS.
     */
    private static int getPageSizeForTableSpaceReferencedInAdviseIndexTable(
            Connection connection) throws SQLException {
        Statement stmt = connection.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT " + "    DISTINCT tbspaceid, "
                + "             pagesize " + "  FROM "
                + "    syscat.tablespaces " + "  WHERE " + "    tbspaceid IN ("
                + "       SELECT " + "           tbspaceid " + "         FROM "
                + "           syscat.tables t, "
                + "           systools.advise_index ai" + "         WHERE "
                + "                t.tabname = ai.tbname " + "   )");

        if (!rs.next())
            throw new SQLException("No indexes in ADVISE_INDEX table");

        int pageSize = rs.getInt(2);

        while (rs.next())
            if (pageSize != rs.getInt(2))
                throw new SQLException(
                        "Can't determine what page size to use: " + pageSize
                                + " or " + rs.getInt(2));

        stmt.close();

        return pageSize;
    }

    // CHECKSTYLE:OFF
    final static String INSERT_INTO_ADVISE_INDEX = "INSERT INTO systools.advise_index ("
            +

            // user metadata... extract it from the system's recommended indexes
            "   EXPLAIN_REQUESTER, "
            + "   TBCREATOR, "
            +

            // table name (string)
            "   TBNAME, "
            +

            // '+A-B+C' means "A" ASC, "B" DESC, "C" ASC ...not sure about
            // INCLUDE columns
            "   COLNAMES, "
            +

            // #Key columns + #Include columns. Must match COLNAMES
            "   COLCOUNT, "
            +

            // 'P' (primary), 'D' (duplicates allowed), 'U' (unique)
            "   UNIQUERULE, "
            +

            // IF unique index THEN #Key columns ELSE -1
            "   UNIQUE_COLCOUNT, "
            +

            // 'Y' or 'N' indicating if reverse scans are supported
            "   REVERSE_SCANS, "
            +

            // 'CLUS', 'REG', 'DIM', 'BLOK'
            "   INDEXTYPE, "
            +

            // The name of the index and the CREATE INDEX statement (must match)
            "   NAME, "
            + "   CREATION_TEXT, "
            +

            // Indicates if the index is real or hypothetical
            // 'Y' or 'N'
            "   EXISTS, "
            +

            // Indicates if the index is system defined... should only be true
            // for real indexes
            // 0, 1, or 2
            "   SYSTEM_REQUIRED, "
            +

            // We use this field to identify an index (also stored locally)
            "   iid, "
            +

            // enable the index for what-if analysis
            // 'Y' or 'N'
            "   USE_INDEX, "
            +

            // statistics, set to -1 to indicate unknown
            "   NLEAF, "
            + "   NLEVELS, "
            + "   FIRSTKEYCARD, "
            + "   FULLKEYCARD, "
            + "   CLUSTERRATIO, "
            + "   AVGPARTITION_CLUSTERRATIO, "
            + "   AVGPARTITION_CLUSTERFACTOR, "
            + "   AVGPARTITION_PAGE_FETCH_PAIRS, "
            + "   DATAPARTITION_CLUSTERFACTOR, "
            + "   CLUSTERFACTOR, "
            + "   SEQUENTIAL_PAGES, "
            + "   DENSITY, "
            + "   FIRST2KEYCARD, "
            + "   FIRST3KEYCARD, "
            + "   FIRST4KEYCARD, "
            + "   PCTFREE, "
            +

            // empty string instead of -1 for this one
            "   PAGE_FETCH_PAIRS, "
            +
            // 0 instead of -1 for this one
            "   MINPCTUSED, "
            +

            /* the rest are likely useless */
            "   EXPLAIN_TIME, "
            + "   CREATE_TIME, "
            + "   STATS_TIME, "
            + "   SOURCE_NAME, "
            + "   REMARKS, "
            + "   CREATOR, "
            + "   DEFINER, "
            + "   SOURCE_SCHEMA, "
            + "   SOURCE_VERSION, "
            + "   EXPLAIN_LEVEL, "
            + "   USERDEFINED, "
            + "   STMTNO, "
            + "   SECTNO, "
            + "   QUERYNO, "
            + "   QUERYTAG, "
            + "   PACKED_DESC, "
            + "   RUN_ID, "
            + "   RIDTOBLOCK, "
            + "   CONVERTED) "
            + " VALUES ("
            + "   'DBTune',"
            + "    ?, "
            + "    ?, "
            + "    ?, "
            + "    ?, "
            + "    ?, "
            + "    ?, "
            + "    ?, "
            + "    ?, "
            + "    ?, "
            + "    ?, "
            + "    ?, "
            + "    ?, "
            + "    ?, "
            + "   'Y', "
            + "   -1, "
            + "   -1, "
            + "   -1, "
            + "   -1, "
            + "   -1, "
            + "   -1, "
            + "   -1, "
            + "   '', "
            + "   -1, "
            + "   -1, "
            + "   -1, "
            + "   -1, "
            + "   -1, "
            + "   -1, "
            + "   -1, "
            + "   -1, "
            + "   '', "
            + "   0 , "
            + "   CURRENT TIMESTAMP, "
            + "   CURRENT TIMESTAMP, "
            + "   NULL, "
            + "   'DBTune', "
            + "   'Created by DBTune', "
            + "   'SYSTEM', "
            + "   'SYSTEM', "
            + "   'NULLID', "
            + "   '', "
            + "   'P', "
            + "   1, "
            + "   1, "
            + "   1, "
            + "   1, "
            + "   '', " + "   NULL, " + "   NULL, " + "   'N', " + "   'Z')";

    final static String SELECT_FROM_EXPLAIN = "SELECT "
            + "     o.operator_id    AS node_id, "
            + "     s2.target_id     AS parent_id, "
            + "     o.operator_type  AS operator_name, "
            + "     s1.object_schema AS object_schema, "
            + "     s1.object_name   AS object_name,"
            + "     s2.stream_count  AS cardinality, "
            + "     o.total_cost     AS cost, "
            + "     cast(cast(s2.column_names AS CLOB(2097152)) AS VARCHAR(2048)) "
            + "                      AS column_names, "
            + "     o.IO_COST    AS iocost, "
            + "     o.CPU_COST    AS cpucost, "
            + "     o.operator_id  AS operator_id, "
            + "     o.FIRST_ROW_COST  AS FIRST_ROW_COST, "
            + "     o.RE_TOTAL_COST  AS RE_TOTAL_COST, "
            + "     o.RE_IO_COST  AS RE_IO_COST, "
            + "     o.RE_CPU_COST  AS RE_CPU_COST, "
            + "     o.BUFFERS  AS BUFFERS "
            + "  FROM "
            + "     systools.explain_operator o "
            + "        LEFT OUTER JOIN "
            + "     systools.explain_stream s2 ON"
            + "               o.operator_id = s2.source_id "
            + "           AND ("
            + "                  s2.target_id >= 0 "
            + "               OR s2.target_id IS NULL "
            + // RETURN op (the root) has NULL as it's parent
            "               )" + "        LEFT OUTER JOIN "
            + "     systools.explain_stream s1 ON "
            + "              o.operator_id = s1.target_id "
            + "          AND o.explain_time = s1.explain_time "
            + "          AND s1.object_name IS NOT NULL " + "  ORDER BY "
            + "     o.operator_id ASC";

    final static String SELECT_FROM_EXPLAIN_FOR_UPDATE = "SELECT "
            + "     o.operator_id   AS node_id, "
            + "     o.operator_type AS operator_name, "
            + "     s.object_schema AS object_schema, "
            + "     s.object_name   AS object_name,"
            + "     s.stream_count  AS cardinality, "
            + "     o.total_cost    AS cost, "
            + "     cast(cast(s.column_names AS CLOB(2097152)) AS VARCHAR(2048)) "
            + "                      AS column_names, "
            + "     o.IO_COST    AS iocost, "
            + "     o.CPU_COST    AS cpucost " + "  FROM "
            + "     systools.explain_operator o, "
            + "     systools.explain_stream s    " + " WHERE "
            + "       o.operator_id   = s.source_id"
            + "   AND (o.operator_type = 'UPDATE'"
            + "         OR o.operator_type = 'INSERT' "
            + "         OR o.operator_type = 'DELETE')"
            + "   AND s.target_id     = -1 ";

    final static String SELECT_FROM_PREDICATES = " SELECT "
            + "   o.operator_id   AS node_id, "
            + "   CAST(CAST(p.predicate_text AS CLOB(2097152)) AS VARCHAR(2048)) "
            + "                   AS predicate_text " + " FROM "
            + "   systools.explain_operator o " + "      LEFT OUTER JOIN     "
            + "   systools.explain_predicate p "
            + "         ON o.operator_id = p.operator_id " + " ORDER BY "
            + "          o.operator_id ASC ";

    final static String DELETE_FROM_ADVISE_INDEX = "DELETE FROM SYSTOOLS.ADVISE_INDEX";

    final static String DELETE_FROM_EXPLAIN_INSTANCE = "DELETE FROM SYSTOOLS.EXPLAIN_INSTANCE";
    // CHECKSTYLE:ON
}