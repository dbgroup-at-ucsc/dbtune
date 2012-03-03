package edu.ucsc.dbtune.inum;

import java.io.PrintWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.plan.InterestingOrder;
import edu.ucsc.dbtune.optimizer.plan.Predicate;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.impl.jdbc.EmbedConnection;

import org.apache.derby.impl.sql.compile.BinaryRelationalOperatorNode;
import org.apache.derby.impl.sql.compile.ColumnReference;
import org.apache.derby.impl.sql.compile.CursorNode;
import org.apache.derby.impl.sql.compile.FromBaseTable;
import org.apache.derby.impl.sql.compile.FromSubquery;
import org.apache.derby.impl.sql.compile.GroupByList;
import org.apache.derby.impl.sql.compile.JoinNode;
import org.apache.derby.impl.sql.compile.OrderByList;
import org.apache.derby.impl.sql.compile.QueryTreeNode;
import org.apache.derby.impl.sql.compile.ResultSetNode;
import org.apache.derby.impl.sql.compile.SelectNode;
import org.apache.derby.impl.sql.compile.SubqueryNode;
import org.apache.derby.impl.sql.compile.ValueNode;

import static edu.ucsc.dbtune.metadata.Index.ASC;

/**
 * Extracts interesting orders by parsing a query using the Derby SQL parser. Supports any SQL
 * statement that is supported by Derby 10.8. This implementation assumes that the SQL statements
 * given to it are well written, i.e. that there are no semantic issues w.r.t. to column names that
 * reference tables from the from list. For example:
 * <pre>
 * {@code .
 * SELECT *
 *   FROM
 *      r,
 *      s
 *   WHERE
 *      r.a = s.a
 *   ORDER BY
 *      a
 * }
 * </pre>
 * <p>
 * The above is ambiguous since the parser can't identify which table the {@code ORDER BY} clause is
 * referring to.
 *
 * @author Ivo Jimenez
 * @author Quoc Trung Tran
 */
public class DerbyInterestingOrdersExtractor implements InterestingOrdersExtractor, Visitor
{
    private static final String CONNECTION_URL = "jdbc:derby:memory:dummy;create=true";
    private static final String DERBY_DEBUG_SETTING = "derby.debug.true";
    private static final String DRIVER_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String LANG_CONNECTION = "LanguageConnectionContext";
    private static final String STOP_AFTER_PARSING =  "StopAfterParsing";
    private static final String STOPPED_AFTER_PARSING = "42Z55";
    private static final Connection CON;
    private static ContextManager cm;
    private static LanguageConnectionContext lcc;

    protected Catalog catalog;
    protected Set<Visitable> astNodes;
    protected boolean defaultAscending;
    
    private SQLStatementParseTree parseTree;

    static {
        System.setProperty(DERBY_DEBUG_SETTING, STOP_AFTER_PARSING);

        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try {
            CON = (EmbedConnection) DriverManager.getConnection(CONNECTION_URL);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        SanityManager.SET_DEBUG_STREAM(new PrintWriter(System.out));

        cm = ((EmbedConnection) CON).getContextManager();
        lcc = (LanguageConnectionContext) cm.getContext(LANG_CONNECTION);
    }

    /**
     * @param catalog
     *      used to retrieve database metadata
     */
    public DerbyInterestingOrdersExtractor(Catalog catalog)
    {
        this.catalog = catalog;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<InterestingOrder> extract(SQLStatement statement) throws SQLException
    {
        reset();

        parse(statement);

        return extract(new BoundSQLStatementParseTree(parseTree, catalog));
    }

    /**
     * Extracts the interesting orders from a {@link BoundSQLStatementParseTree}.
     *
     * @param pt
     *      a bound parse tree
     * @return
     *      a list of interesting orders.
     * @throws SQLException
     *      if an error occurs while obtaining the statement AST; if an error occurs while walking
     *      through the statement AST
     */
    private Set<InterestingOrder> extract(BoundSQLStatementParseTree pt) throws SQLException
    {
        Set<InterestingOrder> ios = new HashSet<InterestingOrder>();
        
        extract(pt, ios);

        return ios;
    }

    /**
     * Extracts recursively interesting orders from a {@link BoundSQLStatementParseTree}.
     *
     * @param pt
     *      a bound parse tree
     * @param ioSet
     *      a Set of interesting orders, where new detected orders are added
     * @throws SQLException
     *      if an error occurs while obtaining the statement AST; if an error occurs while walking
     *      through the statement AST
     */
    private void extract(BoundSQLStatementParseTree pt, Set<InterestingOrder> ioSet)
        throws SQLException
    {
        extractFromGroupByColumns(pt, ioSet);
        extractFromOrderByColumns(pt, ioSet);
        extractFromJoinPredicates(pt, ioSet);

        for (BoundSQLStatementParseTree subquery : pt.getBoundSubqueries())
            extract(subquery, ioSet);
    }

    /**
     * Gets the extractor ready for processing a statement.
     */
    public void reset()
    {
        astNodes = new HashSet<Visitable>();
        parseTree = new SQLStatementParseTree();
    }

    /**
     * Parses the statement by invoking the Derby optimizer. More specifically, it extracts the
     * names of tables referred in the {@code FROM} clause of a {@code SELECT} statement, as well as
     * the column names referred in the {@code ORDER BY} and {@code GROUP BY} clauses.
     * <p>
     * The result is placed in {@link #from}, {@link #groupBy} and {@link #orderBy} members.
     *
     * @param stmt
     *      statement to be parsed
     * @throws SQLException
     *      if an error occurs while obtaining the statement AST; if an error occurs while walking
     *      through the statement AST
     */
    protected void parse(SQLStatement stmt) throws SQLException
    {
        try {
            CON.prepareStatement(stmt.getSQL());
        } catch (SQLException se) {
            String sqlState = se.getSQLState();

            if (!STOPPED_AFTER_PARSING.equals(sqlState))
                throw se;
        }

        QueryTreeNode qt = (QueryTreeNode) lcc.getLastQueryTree();

        try {
            visit(qt);
        } catch (StandardException e) {
            throw new SQLException("An error occurred while walking the query AST", e);
        }

        //qt.treePrint(); // useful for debugging; prints to stdout
    }

    /**
     * Walks the tree to extract the FROM, ORDER BY and GROUP BY lists.
     *
     * @param node
     *      a node of the tree
     * @return
     *      the passed node
     * @throws StandardException
     *      if an error occurs while visiting the node
     */
    @Override
    public Visitable visit(Visitable node) throws StandardException
    {
        if (!astNodes.contains(node)) {
            astNodes.add(node);

            if (node instanceof CursorNode) {
                SelectNode select = (SelectNode) ((CursorNode) node).getResultSetNode();

                if (select.getFromList() != null)
                    select.getFromList().accept(this);

                process(select.getGroupByList());

                process(((CursorNode) node).getOrderByList());

                if (select.getWhereClause() != null)
                    select.getWhereClause().accept(this);

            } else if (node instanceof FromBaseTable) {
                process((FromBaseTable) node);
            } else if (node instanceof FromSubquery) {
                process((FromSubquery) node);
            } else if (node instanceof JoinNode) {
                if (((JoinNode) node).getJoinClause() != null)
                    ((JoinNode) node).getJoinClause().accept(this);
            } else if (node instanceof BinaryRelationalOperatorNode) {
                process((BinaryRelationalOperatorNode) node);
                node.accept(this);
            } else if (node instanceof SubqueryNode) {
                parseTree.addSubquery(process(((SubqueryNode) node).getResultSet()));
            }
        }

        return node;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean skipChildren(Visitable node) throws StandardException
    {
        return node instanceof SubqueryNode;
    }

    /**
     * Processes recursively and returns the subquery contained in the given {@link ResultSetNode}.
     *
     * @param subquery
     *      a result set node
     * @return
     *      the parsed query
     * @throws StandardException
     *      if {@link #parse(SelectNode)} throws an error
     */
    public SQLStatementParseTree process(ResultSetNode subquery) throws StandardException
    {
        return (new DerbyInterestingOrdersExtractor(catalog)).parse((SelectNode) subquery);
    }

    /**
     * Processes the from and where clauses of the subquery.
     *
     * @param subquery
     *      a result set node
     * @return
     *      the parsed query
     * @throws StandardException
     *      if {@link #parse(SelectNode)} throws an error
     */
    public SQLStatementParseTree parse(SelectNode subquery) throws StandardException
    {
        reset();

        if (subquery.getFromList() != null) {
            subquery.getFromList().accept(this);
            visit(subquery.getWhereClause());
        }

        if (subquery.getWhereClause() != null) {
            subquery.getWhereClause().accept(this);
            visit(subquery.getWhereClause());
        }

        return parseTree;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean visitChildrenFirst(Visitable node)
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean stopTraversal()
    {
        return false;
    }

    // CHECKSTYLE:OFF
    private void process(FromBaseTable baseTable) throws StandardException
    {
        // views aren't replaced yet, so they'll be treated as base tables
        parseTree.addFromListTableName(baseTable.getTableName().toString());
    }
    private void process(FromSubquery fromSubquery) throws StandardException
    {
        parseTree.addSubquery(process(fromSubquery.getSubquery()));
    }
    private void process(GroupByList gb)
    {
        if (gb == null)
            return;

        for (int i = 0; i < gb.size(); i++)
            if (gb.getGroupByColumn(i).getColumnExpression().getColumnName() == null)
                continue;
            else if (gb.getGroupByColumn(i).getColumnExpression().getTableName() != null)
                parseTree.addGroupByColumnName(
                        gb.getGroupByColumn(i).getColumnExpression().getTableName() + "." +
                        gb.getGroupByColumn(i).getColumnExpression().getColumnName());
            else
                parseTree.addGroupByColumnName(
                        gb.getGroupByColumn(i).getColumnExpression().getColumnName());
    }
    private void process(OrderByList ob)
    {
        if (ob == null)
            return;

        for (int i = 0; i < ob.size(); i++)
            if (ob.getOrderByColumn(i).getColumnExpression().getColumnName() == null)
                continue;
            else if (ob.getOrderByColumn(i).getColumnExpression().getTableName() != null)
                parseTree.addOrderByColumnName(
                        ob.getOrderByColumn(i).getColumnExpression().getTableName() + "." +
                        ob.getOrderByColumn(i).getColumnExpression().getColumnName(),
                        ob.getOrderByColumn(i).isAscending());
            else
                parseTree.addOrderByColumnName(
                        ob.getOrderByColumn(i).getColumnExpression().getColumnName(),
                        ob.getOrderByColumn(i).isAscending());
    }
    private void process(BinaryRelationalOperatorNode bro) throws StandardException
    {
        ValueNode l = bro.getLeftOperand();
        ValueNode r = bro.getRightOperand();

        if (l instanceof ColumnReference && r instanceof ColumnReference) {
            ColumnReference leftRef = ((ColumnReference) l);
            ColumnReference rightRef = ((ColumnReference) r);

            if (leftRef.getColumnName() == null || rightRef.getColumnName() == null)
                return;

            String leftColumnName;
            String rightColumnName;

            if (leftRef.getTableName() != null)
                leftColumnName = leftRef.getTableName() + "." + leftRef.getColumnName();
            else
                leftColumnName = leftRef.getColumnName();

            if (rightRef.getTableName() != null)
                rightColumnName = rightRef.getTableName() + "." + rightRef.getColumnName();
            else
                rightColumnName = rightRef.getColumnName();

            parseTree.addPredicateText(leftColumnName, rightColumnName);
        }
    }
    private void extractFromGroupByColumns(
            BoundSQLStatementParseTree pt, Set<InterestingOrder> ioSet)
        throws SQLException
    {
        for (Column col : pt.getGroupByColumns())
            ioSet.add(new InterestingOrder(col, ASC));
    }
    private void extractFromJoinPredicates(
            BoundSQLStatementParseTree pt, Set<InterestingOrder> ioSet)
        throws SQLException
    {

        for (Predicate p : pt.getPredicates()) {
            if (!p.getLeftColumn().getTable().equals(p.getRightColumn().getTable())) {
                ioSet.add(new InterestingOrder(p.getLeftColumn(), ASC));
                ioSet.add(new InterestingOrder(p.getRightColumn(), ASC));
            }
        }
        
    }
    private void extractFromOrderByColumns(
            BoundSQLStatementParseTree pt, Set<InterestingOrder> ioSet)
        throws SQLException
    {
        Set<Table> tablesWithOrderingAlreadyCreated = new HashSet<Table>();

        for (Column col : pt.getOrderByColumns().keySet()) {
            if (!tablesWithOrderingAlreadyCreated.contains(col.getTable())) {
                ioSet.add(new InterestingOrder(col, pt.getOrderByColumns().get(col)));
                tablesWithOrderingAlreadyCreated.add(col.getTable());
            }
        }
    }
    // CHECKSTYLE:ON

    /**
     * @author Ivo Jimenez
     */
    public static class SQLStatementParseTree
    {
        private List<SQLStatementParseTree> subqueries;
        private List<String> tableNamesInFrom;
        private List<String> columnNamesInSelect;
        private List<String> columnNamesInGroupBy;
        private Map<String, Boolean> columnNamesInOrderBy;
        private List<PredicateText> predicateText;

        /**
         */
        public SQLStatementParseTree()
        {
            subqueries = new ArrayList<SQLStatementParseTree>();
            tableNamesInFrom = new ArrayList<String>();
            columnNamesInSelect = new ArrayList<String>();
            columnNamesInOrderBy = new LinkedHashMap<String, Boolean>();
            columnNamesInGroupBy = new ArrayList<String>();
            predicateText = new ArrayList<PredicateText>();
        }

        /**
         * Copy constructor.
         *
         * @param other
         *      other parse tree
         */
        public SQLStatementParseTree(SQLStatementParseTree other)
        {
            subqueries = new ArrayList<SQLStatementParseTree>();

            for (SQLStatementParseTree pt : other.getSubqueries())
                subqueries.add(pt);

            tableNamesInFrom = new ArrayList<String>(other.tableNamesInFrom);
            columnNamesInSelect = new ArrayList<String>();
            columnNamesInOrderBy = new LinkedHashMap<String, Boolean>(other.columnNamesInOrderBy);
            columnNamesInGroupBy = new ArrayList<String>(other.columnNamesInGroupBy);
            predicateText = new ArrayList<PredicateText>(other.predicateText);
        }

        /**
         * Gets the subqueries for this instance.
         *
         * @return The subqueries.
         */
        public List<SQLStatementParseTree> getSubqueries()
        {
            return subqueries;
        }

        /**
         * Gets the subqueries for this instance.
         *
         * @return The subqueries.
         */
        public List<PredicateText> getPredicateText()
        {
            return predicateText;
        }

        /**
         * Gets the tableNames for this instance.
         *
         * @return The tableNames.
         */
        public List<String> getFromListTableNames()
        {
            return tableNamesInFrom;
        }

        /**
         * Gets the columnNamesInSelect for this instance.
         *
         * @return The columnNamesInSelect.
         */
        public List<String> getSelectColumnNames()
        {
            return columnNamesInSelect;
        }

        /**
         * Gets the columnNamesInOrderBy for this instance.
         *
         * @return The columnNamesInOrderBy.
         */
        public Map<String, Boolean> getOrderByColumnNames()
        {
            return this.columnNamesInOrderBy;
        }

        /**
         * Gets the columnNamesInGroupBy for this instance.
         *
         * @return The columnNamesInGroupBy.
         */
        public List<String> getGroupByColumnNames()
        {
            return columnNamesInGroupBy;
        }

        /**
         * Adds a new subquery.
         *
         * @param subquery
         *      a subquery for this statement
         */
        public void addSubquery(SQLStatementParseTree subquery)
        {
            subqueries.add(subquery);
        }

        /**
         * Adds the name of a table for this instance.
         *
         * @param tableName
         *      name of the table
         */
        public void addFromListTableName(String tableName)
        {
            tableNamesInFrom.add(tableName);
        }

        /**
         * Adds the name of a column in the select for this instance.
         *
         * @param columnName
         *      column names in select clause
         */
        public void addSelectColumnName(String columnName)
        {
            columnNamesInSelect.add(columnName);
        }

        /**
         * Adds the name of a column to the order by.
         *
         * @param columnName
         *      column names in order by
         * @param ascending
         *      ascending value
         */
        public void addOrderByColumnName(String columnName, boolean ascending)
        {
            columnNamesInOrderBy.put(columnName, ascending);
        }

        /**
         * Adds the name of a column to the group by.
         *
         * @param columnName
         *      column names in group by
         */
        public void addGroupByColumnName(String columnName)
        {
            columnNamesInGroupBy.add(columnName);
        }

        /**
         * Adds the text of a predicate, along with the operator.
         *
         * @param a
         *      left operand in predicate
         * @param b
         *      right operand in predicate
         */
        public void addPredicateText(String a, String b)
        {
            predicateText.add(new PredicateText(a, b));
        }
    }

    /**
     * @author Ivo Jimenez
     */
    public static class PredicateText
    {
        private String columnA;
        private String columnB;

        /**
         * Text for a join predicate.
         *
         * @param columnA
         *      first column
         * @param columnB
         *      second column
         */
        public PredicateText(String columnA, String columnB)
        {
            this.columnA = columnA;
            this.columnB = columnB;
        }

        /**
         * Gets the columnA for this instance.
         *
         * @return The columnA.
         */
        public String getColumnA()
        {
            return this.columnA;
        }

        /**
         * Gets the columnB for this instance.
         *
         * @return The columnB.
         */
        public String getColumnB()
        {
            return this.columnB;
        }
    }

    /**
     */
    public static class BoundSQLStatementParseTree extends SQLStatementParseTree
    {
        private List<Table> tables;
        private List<Column> columnsInSelect;
        private Map<Column, Boolean> columnsInOrderBy;
        private List<Column> columnsInGroupBy;
        private List<BoundSQLStatementParseTree> boundSubqueries;
        private List<Predicate> predicates;
        private Catalog catalog;

        /**
         * Constructor.
         *
         * @param pt
         *      a parse tree
         * @param catalog
         *      the catalog used to get metadata when binding
         * @throws SQLException
         *      if an object can't be bound
         */
        public BoundSQLStatementParseTree(SQLStatementParseTree pt, Catalog catalog)
            throws SQLException
        {
            super(pt);

            this.catalog = catalog;

            boundSubqueries = new ArrayList<BoundSQLStatementParseTree>();

            tables = bindTables(getFromListTableNames());

            for (SQLStatementParseTree subquery : getSubqueries())
                boundSubqueries.add(new BoundSQLStatementParseTree(subquery, catalog, tables));

            columnsInSelect = bindColumns(tables, new ArrayList<Table>(), getSelectColumnNames());
            columnsInGroupBy = bindColumns(tables, new ArrayList<Table>(), getGroupByColumnNames());
            columnsInOrderBy = bindColumns(tables, new ArrayList<Table>(), getOrderByColumnNames());

            predicates = bindPredicates(tables, new ArrayList<Table>(), getPredicateText());

        }

        /**
         * Constructor for subqueries.
         *
         * @param pt
         *      a parse tree
         * @param catalog
         *      the catalog used to get metadata when binding
         * @param tablesFromUpper
         *      tables from one level up
         * @throws SQLException
         *      if an object can't be bound
         */
        public BoundSQLStatementParseTree(
                SQLStatementParseTree pt, Catalog catalog, List<Table> tablesFromUpper)
            throws SQLException
        {
            super(pt);

            this.catalog = catalog;

            boundSubqueries = new ArrayList<BoundSQLStatementParseTree>();

            tables = bindTables(getFromListTableNames());

            for (SQLStatementParseTree subquery : getSubqueries())
                boundSubqueries.add(new BoundSQLStatementParseTree(subquery, catalog, tables));

            columnsInSelect = bindColumns(tables, tablesFromUpper, getSelectColumnNames());
            columnsInGroupBy = bindColumns(tables, tablesFromUpper, getGroupByColumnNames());
            columnsInOrderBy = bindColumns(tables, tablesFromUpper, getOrderByColumnNames());

            predicates = bindPredicates(tables, tablesFromUpper, getPredicateText());
        }

        /**
         * Binds a column list.
         *
         * @param tables
         *      list of tables where to find column namesj
         * @param tablesFromUpper
         *      tables from one level up
         * @param columnNames
         *      list of column names
         * @throws SQLException
         *      if an object can't be bound
         * @return
         *      the list of columns bound
         */
        private List<Column> bindColumns(
                List<Table> tables, List<Table> tablesFromUpper, List<String> columnNames)
            throws SQLException
        {
            List<Column> columns = new ArrayList<Column>();

            for (String columnName : columnNames)
                try {
                    columns.add(bindColumn(tables, tablesFromUpper, columnName));
                } catch (SQLException e) {
                    e.getMessage();
                }

            return columns;
        }

        /**
         * Binds a column that may or may not be fully qualified. If it's not, it is search on every 
         * table.
         *
         * @param tables
         *      list of tables where to optionally look for, if the name of the column is not fully 
         *      qualified
         * @param tablesFromUpper
         *      tables from one level up
         * @param columnName
         *      name of the column to bind
         * @return
         *      the corresponding column object
         * @throws SQLException
         *      if the column is not found; if more than one column matches
         */
        private Column bindColumn(
                List<Table> tables, List<Table> tablesFromUpper, String columnName)
            throws SQLException
        {
            List<Table> allTables = new ArrayList<Table>();
            Column column = null;

            allTables.addAll(tables);
            allTables.addAll(tablesFromUpper);

            // first with fully qualified name
            if (columnName.split("\\.").length == 3) {
                // we have "schema.table.column"
                column = catalog.<Column>findByName(columnName);

            } else if (columnName.split("\\.").length == 2) {
                // we have "table.column"
                String tableName = columnName.split("\\.")[0];
                String colName = columnName.split("\\.")[1];

                for (Table table : allTables) {
                    if (table.getName().equalsIgnoreCase(tableName))
                        column = table.findColumn(colName);

                    if (column != null)
                        break;
                }
            }

            if (column != null)
                return column;

            // then try to search the first onefrom column names
            for (Table table : allTables) {
                if (table.findColumn(columnName) != null &&
                        !tablesFromUpper.contains(table) &&
                        column != null)
                    throw new SQLException("More than one table with " + columnName + " as member");

                column = table.findColumn(columnName);

                if (column != null)
                    break;
            }

            if (column == null)
                throw new SQLException("Can't find " + columnName + " in catalog");

            return column;
        }

        /**
         * Binds a column list.
         *
         * @param tables
         *      list of tables where to find column namesj
         * @param tablesFromUpper
         *      tables from one level up
         * @param columnNames
         *      list of column names
         * @throws SQLException
         *      if an object can't be bound
         * @return
         *      the list of columns bound
         */
        private Map<Column, Boolean> bindColumns(
                List<Table> tables, List<Table> tablesFromUpper, Map<String, Boolean> columnNames)
            throws SQLException
        {
            Map<Column, Boolean> columns = new LinkedHashMap<Column, Boolean>();

            for (Map.Entry<String, Boolean> entry : columnNames.entrySet())
                try {
                    columns.put(
                            bindColumn(tables, tablesFromUpper, entry.getKey()), entry.getValue());
                } catch (SQLException e) {
                    e.getMessage();
                }

            return columns;
        }

        /**
         * Binds a column list.
         *
         * @param tableNames
         *      list of table names, fully qualified
         * @throws SQLException
         *      if an object can't be bound
         * @return
         *      the list of tables bound
         */
        private List<Table> bindTables(List<String> tableNames) throws SQLException
        {
            List<Table> tables = new ArrayList<Table>();

            for (String tableName : tableNames) {
                Table table = catalog.<Table>findByName(tableName);

                if (table == null)
                    throw new SQLException("Can't find " + tableName + " in catalog");

                tables.add(table);
            }

            return tables;
        }

        /**
         * Binds a list of join predicates.
         *
         * @param tables
         *      list of tables where to find column namesj
         * @param tablesFromUpper
         *      tables from one level up
         * @param predicateText
         *      list of predicate text, where column names are fully qualified
         * @throws SQLException
         *      if an object can't be bound
         * @return
         *      the list of predicates bound
         */
        private List<Predicate> bindPredicates(
                List<Table> tables, List<Table> tablesFromUpper, List<PredicateText> predicateText)
            throws SQLException
        {
            List<Predicate> predicates = new ArrayList<Predicate>();

            for (PredicateText pt : predicateText)
                predicates.add(
                        new Predicate(
                            bindColumn(tables, tablesFromUpper, pt.getColumnA()),
                            bindColumn(tables, tablesFromUpper, pt.getColumnB())));

            return predicates;
        }

        /**
         * Returns bound subqueries.
         *
         * @return
         *      subqueries
         */
        public List<BoundSQLStatementParseTree> getBoundSubqueries()
        {
            return this.boundSubqueries;
        }

        /**
         * Gets the predicates for this instance.
         *
         * @return The predicates.
         */
        public List<Predicate> getPredicates()
        {
            return this.predicates;
        }

        /**
         * Gets the tables for this instance.
         *
         * @return The tables.
         */
        public List<Table> getTables()
        {
            return this.tables;
        }

        /**
         * Gets the columnsInSelect for this instance.
         *
         * @return The columnsInSelect.
         */
        public List<Column> getSelectColumns()
        {
            return this.columnsInSelect;
        }

        /**
         * Gets the columnsInOrderBy for this instance.
         *
         * @return The columnsInOrderBy.
         */
        public Map<Column, Boolean> getOrderByColumns()
        {
            return this.columnsInOrderBy;
        }

        /**
         * Gets the columnsInGroupBy for this instance.
         *
         * @return The columnsInGroupBy.
         */
        public List<Column> getGroupByColumns()
        {
            return this.columnsInGroupBy;
        }
    }
}
