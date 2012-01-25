package edu.ucsc.dbtune.inum;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.sql.compile.CursorNode;
import org.apache.derby.impl.sql.compile.FromBaseTable;
import org.apache.derby.impl.sql.compile.FromList;
import org.apache.derby.impl.sql.compile.FromSubquery;
import org.apache.derby.impl.sql.compile.GroupByList;
import org.apache.derby.impl.sql.compile.OrderByList;
import org.apache.derby.impl.sql.compile.QueryTreeNode;
import org.apache.derby.impl.sql.compile.SelectNode;

import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;

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

    private Catalog catalog;
    private FromList from;
    private GroupByList groupBy;
    private OrderByList orderBy;
    private Set<Visitable> astNodes;
    private boolean defaultAscending;

    static {
        System.setProperty(DERBY_DEBUG_SETTING, STOP_AFTER_PARSING);

        try {
            Class.forName(DRIVER_NAME);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try {
            CON = (EmbedConnection) DriverManager.getConnection(CONNECTION_URL);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param catalog
     *      used to retrieve database metadata
     * @param mode
     *      the default value assigned to columns appearing in {@code GROUP BY} but not in the 
     *      {@code ORDER BY} clause. Possible values are {@link Index#ASCENDING} and {@link 
     *      Index#DESCENDING}
     */
    public DerbyInterestingOrdersExtractor(Catalog catalog, boolean mode)
    {
        this.catalog = catalog;
        this.defaultAscending = mode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Set<Index>> extract(SQLStatement statement) throws SQLException
    {
        parse(statement);

        try {
            return extractInterestingOrders();
        }
        catch (StandardException e) {
            throw new SQLException("An error occurred while iterating the from list", e);
        }
    }

    /**
     * Determines if this instance is defaultAscending.
     *
     * @return The defaultAscending.
     */
    public boolean isDefaultAscending()
    {
        return this.defaultAscending;
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
    private void parse(SQLStatement stmt) throws SQLException
    {
        ContextManager cm;
        LanguageConnectionContext lcc;
        QueryTreeNode qt;

        try {
            CON.prepareStatement(stmt.getSQL());
        }
        catch (SQLException se) {
            String sqlState = se.getSQLState();

            if (!STOPPED_AFTER_PARSING.equals(sqlState))
                throw se;
        }

        cm = ((EmbedConnection) CON).getContextManager();
        lcc = (LanguageConnectionContext) cm.getContext(LANG_CONNECTION);
        qt = (QueryTreeNode) lcc.getLastQueryTree();

        astNodes = new HashSet<Visitable>();
        from = null;
        groupBy = null;
        orderBy = null;

        try {
            visit(qt);
        }
        catch (StandardException e) {
            throw new SQLException("An error occurred while walking the query AST", e);
        }
        //qt.treePrint(); // useful for debugging; prints to stdout
    }

    /**
     * Extracts the interesting orders from the previously populated {@link #from}, {@link #groupBy} 
     * and {@link #orderBy} members. At the same time, this method binds database object names to 
     * their in-memory representation (by consulting the {@link Catalog} object that was provided to 
     * the constructor).
     * <p>
     * For columns specified in the {@code GROUP BY} but not contained in the {@code ORDER BY}, the 
     * default sorting order is given by {@link #isDefaultAscending}.
     *
     * @return
     *      a list of sets of interesting orders, one per every table referenced in the statement
     * @throws StandardException
     *      if an error occurs while iterating the from list
     * @throws SQLException
     *      if a {@code SELECT} statement contains subqueries; if {@link 
     *      #extractInterestingOrdersPerTable} fails; 
     */
    private List<Set<Index>> extractInterestingOrders() throws StandardException, SQLException
    {
        List<String> tableNames = new ArrayList<String>();
        Set<String> columnNames = new HashSet<String>();
        Map<String, Boolean> ascending = new HashMap<String, Boolean>();
        String col;

        if (from == null || from.size() == 0)
            throw new SQLException("null or empty FROM list");

        if (from.size() == 1 && orderBy == null && groupBy == null)
            throw new SQLException(
                "Can't extract for single-table queries without ORDER BY or GROUP BY clauses");

        if (from.size() > 1)
            throw new SQLException("Can't extract for joins yet");

        for (int i = 0; i < from.size(); i++) {
            if (from.elementAt(i) instanceof FromBaseTable)
                tableNames.add(((FromBaseTable) from.elementAt(i)).getTableName().toString());
            else if (from.elementAt(i) instanceof FromSubquery)
                throw new SQLException("Can't handle subqueries yet");
        }

        if (orderBy != null) {
            for (int i = 0; i < orderBy.size(); i++) {
                col = orderBy.getOrderByColumn(i).getColumnExpression().getColumnName();

                columnNames.add(col);
                ascending.put(col, orderBy.getOrderByColumn(i).isAscending());
            }
        }

        if (groupBy != null) {
            for (int i = 0; i < groupBy.size(); i++) {
                col = groupBy.getGroupByColumn(i).getColumnExpression().getColumnName();

                if (columnNames.add(col))
                    ascending.put(col, isDefaultAscending());
            }
        }

        return extractInterestingOrdersPerTable(tableNames, columnNames, ascending);
    }

    /**
     * Binds database object names to their in-memory representation (by consulting the {@link 
     * Catalog} object that was provided to the constructor). This method assumes that the SQL text 
     * is well written, i.e. that there are no ambiguities with respect to the column names referred 
     * in the {@code ORDER BY} and {@code GROUP BY} clauses.
     *
     * @param tableNames
     *      each element corresponds to a table in the {@code FROM} clause
     * @param columnNames
     *      each element corresponds to a column in the {@code GROUP BY} or {@code ORDER BY} clause
     * @param ascending
     *      for every referenced column name, a corresponding entry in the map should be included; 
     *      the boolean value corresponds the ordering
     * @return
     *      a list of sets of interesting orders, one per every table referenced in the statement
     * @throws SQLException
     *      if a column can't be found in the catalog
     */
    private List<Set<Index>> extractInterestingOrdersPerTable(
            List<String> tableNames, Set<String> columnNames, Map<String, Boolean> ascending)
        throws SQLException
    {
        Map<Table, Set<Index>> interestingOrdersPerTable;
        Set<Index> interestingOrdersForTable;
        Column col;

        interestingOrdersPerTable = new HashMap<Table, Set<Index>>();

        for (String colName : columnNames) {

            col = null;

            for (String tblName : tableNames) {

                try {
                    col = catalog.<Column>findByName(tblName + "." + colName);
                }
                catch (SQLException e) {
                    // it's OK not to find it, as long as we find it in a subsequent iteration
                    e = null;
                }

                if (col == null)
                    continue;

                interestingOrdersForTable = interestingOrdersPerTable.get(col.getTable());

                if (interestingOrdersForTable == null) {
                    interestingOrdersForTable = new HashSet<Index>();

                    // add the empty interesting order
                    interestingOrdersForTable.add(getFullTableScanIndexInstance(col.getTable()));
                    
                    interestingOrdersPerTable.put(col.getTable(), interestingOrdersForTable);
                }

                interestingOrdersForTable.add(new InterestingOrder(col, ascending.get(colName)));

                // we found the column, so let's look for the next one (we assume the SQL is well 
                // written, i.e. that the reference to a column is not ambiguous).
                break;
                // Alternatively, we could go to the next loop and check if we found that the column 
                // is also in another table and then throw an ambiguity exception, but we don't 
                // bother for now since we assume that the SQL query is well written (otherwise the 
                // underlying DBMS would be throwing an exception)
            }

            if (col == null)
                // not found in any table, so we don't know how to proceed
                throw new SQLException("Can't find column " + colName + " in catalog");
        }
        
        return new ArrayList<Set<Index>>(interestingOrdersPerTable.values());
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

            if (node instanceof SelectNode) {
                final SelectNode select = (SelectNode) node;
                from = select.getFromList();
                groupBy = select.getGroupByList();
            } else if (node instanceof CursorNode) {
                orderBy = ((CursorNode) node).getOrderByList();
            }

            node.accept(this);
        }

        return node;
    }

    /**
     * {@inheritDoc}
     */
    public boolean visitChildrenFirst(Visitable node)
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public boolean stopTraversal()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public boolean skipChildren(Visitable node) throws StandardException
    {
        return false;
    }
}
