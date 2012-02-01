package edu.ucsc.dbtune.inum;


import edu.ucsc.dbtune.optimizer.plan.InterestingOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

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
import org.apache.derby.impl.sql.compile.BinaryRelationalOperatorNode;
import org.apache.derby.impl.sql.compile.ColumnReference;
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

    private Catalog catalog;
    private FromList from;
    private GroupByList groupBy;
    private OrderByList orderBy;
    private Set<Visitable> astNodes;
    private boolean defaultAscending;
    
    private List<ColumnReference> binaryOperandNodes;
    boolean leftOperand, rightOperand;
    
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
        binaryOperandNodes = new ArrayList<ColumnReference>();
        leftOperand = rightOperand = false;
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
        Column col; 
        String colName;
        List<String> tableNames = new ArrayList<String>();
        List<Column> listCols = new ArrayList<Column>();
        Map<Column, Boolean> ascending = new HashMap<Column, Boolean>();        
        Map<Table, List<Column>> mapTableColumn = new HashMap<Table, List<Column>>();
        Map<Column, Integer> mapInterestingOrderColumn = new HashMap<Column, Integer>();

        if (from == null || from.size() == 0)
            throw new SQLException("null or empty FROM list");
        
        // Add an empty interesting order
        if (from.size() == 1 && orderBy == null && groupBy == null) {
            String tblName = ((FromBaseTable) from.elementAt(0)).getTableName().toString();
            Table table = catalog.<Table>findByName(tblName);
            mapTableColumn.put(table, new ArrayList<Column>());
            return extractInterestingOrdersPerTable(mapTableColumn, ascending);
        }

        for (int i = 0; i < from.size(); i++) {
            if (from.elementAt(i) instanceof FromBaseTable)
                tableNames.add(((FromBaseTable) from.elementAt(i)).getTableName().toString());
            else if (from.elementAt(i) instanceof FromSubquery) 
                throw new SQLException("Can't handle subqueries yet");
        }
        
        // Order by attributes
        if (orderBy != null) {
            listCols.clear();
            for (int i = 0; i < orderBy.size(); i++) {
                colName = orderBy.getOrderByColumn(i).getColumnExpression().getColumnName(); 
                col = bindColumnToTable(tableNames, colName);
                // TODO: strictly speaking, we need to match the unknown column
                // with the alias column in the select-clause
                if (col == null)
                    continue;
                                
                ascending.put(col, orderBy.getOrderByColumn(i).isAscending());
                listCols.add(col);
            }
            bindColumnToTable(listCols, mapTableColumn);
            
            // keep only the first column in each table
            // due to the semantic of ORDER BY
            for (Entry<Table, List<Column>> entry : mapTableColumn.entrySet()){
                List<Column> ls = new ArrayList<Column>();
                Column column = entry.getValue().get(0);
                ls.add(column);                
                entry.setValue(ls);
                mapInterestingOrderColumn.put(column, new Integer(1));
            }         
        }
        
        // Group by attributes
        if (groupBy != null) {
            listCols.clear();
            for (int i = 0; i < groupBy.size(); i++) {                
                colName = groupBy.getGroupByColumn(i).getColumnExpression().getColumnName();               
                col = bindColumnToTable(tableNames, colName);
                if (col == null) 
                    continue;
                
                if (mapInterestingOrderColumn.containsKey(col) == false){
                    mapInterestingOrderColumn.put(col, new Integer(1));
                    ascending.put(col, isDefaultAscending());
                    listCols.add(col);
                }
            }
            bindColumnToTable(listCols, mapTableColumn);
        }
        
        // Join predicates
        if (binaryOperandNodes.size() > 0) {
            listCols.clear();
            for (ColumnReference colRef: binaryOperandNodes) {
                colName = colRef.getColumnName();
                col = null;
                // p_partkey = ps_partkey
                if (colRef.getTableName() == null) 
                    col = this.bindColumnToTable(tableNames, colName);
                else {
                    // table_0.column_0 = table_1.column_0    
                    // We need to retrieve the table name of each {@ColumnReference} object
                    // for not ambiguous which relation the column {column_0} refers to
                    for (String tblName : tableNames) {
                        if (tblName.contains(colRef.getTableName()) == true) {
                            colName = tblName + "." + colName;                        
                            col = catalog.<Column>findByName(colName);
                            break;
                        }
                    }
                }
                
                if (col == null) 
                    continue;
                
                if (mapInterestingOrderColumn.containsKey(col) == false){
                    mapInterestingOrderColumn.put(col, new Integer(1));
                    ascending.put(col, isDefaultAscending());
                    listCols.add(col);
                }
            }
            bindColumnToTable(listCols, mapTableColumn);
        }

        return extractInterestingOrdersPerTable(mapTableColumn, ascending);
    }

    /**
     * Binds database object names to their in-memory representation (by consulting the {@link 
     * Catalog} object that was provided to the constructor). This method assumes that the SQL text 
     * is well written, i.e. that there are no ambiguities with respect to the column names referred 
     * in the {@code ORDER BY} and {@code GROUP BY} clauses.
     *
     * @param tableNames
     *      each element corresponds to a table in the {@code FROM} clause
     * @param columnName
     *      corresponds to a column in the {@code GROUP BY} or {@code ORDER BY} clause     
     * @return
     *      A {@Column} object or NULL if the column is not a part of all the tables
     *      in @code tableNames} 
     */
    private Column bindColumnToTable(List<String> tableNames, String colName)
    {
        Column col = null;
        
        for (String tblName : tableNames) {
            try {
                col = catalog.<Column>findByName(tblName + "." + colName);
            }
            catch (SQLException e) {
                // it's OK not to find it, as long as we find it in a subsequent iteration
                e = null;
            }
            if (col != null) {
                break;
            }
        }
        
        return col;
    }
    
    /**
     * Place columns into the mapping of the table that contains this column
     * 
     * @param listCols
     *      The list of columns
     * @param bindTable
     *      The mapping 
     *      
     * {\bf Note}: The variable {@bindTable} might be changed after this method is called.     
     */
    private void bindColumnToTable(List<Column> listCols, Map<Table, List<Column>> bindTable)
    {   
        for (Column col : listCols) {           
            List<Column> lc = bindTable.get(col.getTable());
            if (lc == null) {
                List<Column> newListCols = new ArrayList<Column>();
                newListCols.add(col);
                bindTable.put(col.getTable(), newListCols);
            } else 
                lc.add(col);
        }
    }
    
    /**
     * Extract interesting orders 
     * 
     * @param mapTableColumn
     *      Map each table to a list of interesting columns that belongs to this table
     * @param ascending
     *      The ascending order of each interesting column
     * @return
     *      A set of interesting orders
     * @throws SQLException
     *      when there is error in connecting with databases     
     */
    private List<Set<Index>> extractInterestingOrdersPerTable(
            Map<Table, List<Column>> mapTableColumn, Map<Column, Boolean> ascending) 
            throws SQLException
    {   
        Map<Table, Set<Index>> interestingOrdersPerTable = new HashMap<Table, Set<Index>>();        
        Set<Index> interestingOrdersForTable;
        
        for (Entry<Table, List<Column>> entry : mapTableColumn.entrySet()){
            interestingOrdersForTable = new HashSet<Index>();
            
            for (Column col : entry.getValue()) 
                interestingOrdersForTable.add(new InterestingOrder(col, ascending.get(col)));
            
            // add full table scan
            interestingOrdersForTable.add(getFullTableScanIndexInstance(entry.getKey()));
            interestingOrdersPerTable.put(entry.getKey(), interestingOrdersForTable);
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
            
            /**
             * The algorithm to derive the columns in the join predicates work as follows:              
             * Whenever we encounter {@code BinaryRelationalOperatorNode} object, we are expected
             * to receive the next two nodes are the left operand and right operand.
             * We then need to check if both the left and right operand are of type 
             * {code ColumnReference}, then this operator node corresponds to a join predicate.
             */            
            if (leftOperand == true) {
                // If the left operand is a column, we will investigate the right operand
                // to determine a join predicate.
                // Otherwise, we simply skip this BinaryOperator. 
                if (node instanceof ColumnReference){
                    binaryOperandNodes.add((ColumnReference) node);
                    rightOperand = true;
                } else 
                    rightOperand = false;
                
                leftOperand = false;
            } else if (rightOperand == true) {                
                if (node instanceof ColumnReference)// this is matched join predicate
                    binaryOperandNodes.add((ColumnReference) node);
                else // if not, remove the left operand stored in the list
                    binaryOperandNodes.remove(binaryOperandNodes.size() - 1);
                
                rightOperand = false;
            }
            if (node instanceof SelectNode) {
                final SelectNode select = (SelectNode) node;
                from = select.getFromList();
                groupBy = select.getGroupByList();
            } else if (node instanceof CursorNode) 
                orderBy = ((CursorNode) node).getOrderByList();
            else if (node instanceof BinaryRelationalOperatorNode) {
                leftOperand = true;
                rightOperand = false;
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
