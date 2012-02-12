package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.BinaryRelationalOperatorNode;
import org.apache.derby.impl.sql.compile.ColumnReference;
import org.apache.derby.impl.sql.compile.CursorNode;
import org.apache.derby.impl.sql.compile.FromBaseTable;
import org.apache.derby.impl.sql.compile.HalfOuterJoinNode;
import org.apache.derby.impl.sql.compile.JoinNode;
import org.apache.derby.impl.sql.compile.SelectNode;
import org.apache.derby.impl.sql.compile.ResultColumn;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;

public class DerbyReferencedColumnsExtractor extends DerbyInterestingOrdersExtractor 
{
    private Set<ColumnReference> columnsInPredicate;
    private Set<ResultColumn> columnsInSelectClause;
    
    /**
     * @param catalog
     *      used to retrieve database metadata
     * @param mode
     *      the default value assigned to columns appearing in {@code GROUP BY} but not in the
     *      {@code ORDER BY} clause. Possible values are {@link Index#ASCENDING} and {@link
     *      Index#DESCENDING}
     */
    public DerbyReferencedColumnsExtractor(Catalog catalog, boolean mode)
    {
       super(catalog, mode);
    }

    /**
     * Extract all columns that are referenced in the select, from, where, order by, 
     * and group-by clauses of the given statement.
     * 
     * @param statement
     *      The given SQLStatement to parse
     *      
     * @return
     *      A map of referenced columns to the order (ascending or descending)
     * 
     * @throws StandardException
     *      if an error occurs while iterating the from list
     * @throws SQLException
     *      if the binding to database object fails
     */
    public Map<Column, Boolean> getReferencedColumn(SQLStatement statement) throws SQLException, StandardException 
    {
        columnsInPredicate = new HashSet<ColumnReference>();
        columnsInSelectClause = new HashSet<ResultColumn>();
        super.parse(statement);
        
        Map<Column, Boolean> ascending = new HashMap<Column, Boolean>();
        String colName;
        Column col;
        List<String> tableNames = new ArrayList<String>();
        
        if (from == null || from.size() == 0)
            throw new SQLException("null or empty FROM list");

        for (int i = 0; i < from.size(); i++) {
            
            if (from.elementAt(i) instanceof FromBaseTable)
                tableNames.add(((FromBaseTable) from.elementAt(i)).getTableName().toString());
        }

        // bind order by first
        if (orderBy != null) { 
            for (int i = 0; i < orderBy.size(); i++) {
                
                colName = orderBy.getOrderByColumn(i).getColumnExpression().getColumnName();
                col = bindColumn(tableNames, colName);
                
                if (col == null)
                    continue;
    
                ascending.put(col, orderBy.getOrderByColumn(i).isAscending());
            }
        }     
            
        // bind group by list
        if (groupBy != null) {
            for (int i = 0; i < groupBy.size(); i++) {
                
                colName = groupBy.getGroupByColumn(i).getColumnExpression().getColumnName();
                col = bindColumn(tableNames, colName);
                if (col == null)
                    continue;
                
                if (!ascending.containsKey(col))
                    ascending.put(col, isDefaultAscending());
            }
        }
        
        // bind from list
        for (ColumnReference colRef : columnsInPredicate) {
            
            colName = colRef.getColumnName();
            col = bindColumn(tableNames, colName);
            if (col == null)
                continue;
            
            if (!ascending.containsKey(col))
                ascending.put(col, isDefaultAscending());
        }
        
        // bind select list
        for (ResultColumn colResult : columnsInSelectClause) {
            
            colName = colResult.getName();
            col = bindColumn(tableNames, colName);            
            if (col == null)
                continue;
            
            if (!ascending.containsKey(col))
                ascending.put(col, isDefaultAscending());
        }
        
        return ascending;
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
            
            if (leftOperand == true) {
                
                if (node instanceof ColumnReference)
                    columnsInPredicate.add((ColumnReference) node);

                leftOperand = false;
                rightOperand = true;
                
            } else if (rightOperand == true) {
                
                if (node instanceof ColumnReference) 
                    columnsInPredicate.add((ColumnReference) node);
                
                rightOperand = false;
            }            
            
            else if (node instanceof ResultColumn)
                columnsInSelectClause.add((ResultColumn) node);
            
            else if (node instanceof SelectNode) {
                
                final SelectNode select = (SelectNode) node;
                
                if (from == null)
                    from = select.getFromList();
                else {
                    
                    for (int i = 0; i < select.getFromList().size(); i++) {
                        
                        if (select.getFromList().elementAt(i) instanceof FromBaseTable)
                            from.addFromTable((FromBaseTable) select.getFromList().elementAt(i));
                        // Extract the table from the left and right result set
                        // of a join node instance
                        else if (select.getFromList().elementAt(i) instanceof HalfOuterJoinNode) {
                            
                            HalfOuterJoinNode outerJoin = (HalfOuterJoinNode) 
                                                            select.getFromList().elementAt(i);                            
                            from.addFromTable((FromBaseTable)outerJoin.getLeftResultSet());
                            from.addFromTable((FromBaseTable)outerJoin.getRightResultSet());
                        }
                        
                        else if (select.getFromList().elementAt(i) instanceof JoinNode) {
                            
                            JoinNode outerJoin = (JoinNode) select.getFromList().elementAt(i);
                            from.addFromTable((FromBaseTable)outerJoin.getLeftResultSet());
                            from.addFromTable((FromBaseTable)outerJoin.getRightResultSet());
                        }
                    }
                }
                
                if (groupBy == null)
                    groupBy = select.getGroupByList();
                
            } else if (node instanceof CursorNode) {
                
                if (orderBy == null)
                    orderBy = ((CursorNode) node).getOrderByList();
                
            } else if (node instanceof BinaryRelationalOperatorNode) {
                
                leftOperand = true;
                rightOperand = false;
            } 
                
            node.accept(this);
        }

        return node;
    }
}
