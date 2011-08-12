package edu.ucsc.dbtune.inum.model;

import Zql.ZConstant;
import Zql.ZExp;
import Zql.ZExpression;
import Zql.ZFromItem;
import Zql.ZGroupBy;
import Zql.ZOrderBy;
import Zql.ZQuery;
import Zql.ZSelectItem;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import edu.ucsc.dbtune.inum.Config;
import edu.ucsc.dbtune.inum.autopilot.PostgresGlobalInfo;
import edu.ucsc.dbtune.inum.commons.ZqlUtils;
import edu.ucsc.dbtune.util.Checks;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.log4j.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: Debabrata Dash
 * Date: Feb 24, 2006
 * Time: 9:18:12 PM
 * The file is a copyright of CMU, all rights reserved.
 */
public class ColumnsGatherer {
    private ZQuery query;
    private Set allColumns;
    private Set eqColumns;
    private Set joinColumns;
    private LinkedHashSet groupByColumns;
    private LinkedHashSet orderByColumns;
    private HashSet whereColumns;

    private static Properties columnProperties;
//    public static final String COLUMN_PROPERTIES = Joiner.on(File.separator).join(
//        Config.HOME, "config", "column.properties." + Config.getDatabaseName());
    //public static final String COLUMN_PROPERTIES = "column.properties";
    private static Logger log = Logger.getLogger(ColumnsGatherer.class);

   private static final AtomicReference<PostgresGlobalInfo> GLOBAL_INFO = new AtomicReference<PostgresGlobalInfo>();

    public ColumnsGatherer(ZQuery query) {
      this(query, new PostgresGlobalInfo());
    }

    public ColumnsGatherer(ZQuery query, PostgresGlobalInfo globalInfo){
        this.query = query;
      if (query == null)
            return;

        GLOBAL_INFO.set(Checks.checkNotNull(globalInfo));
        allColumns = new HashSet();
        eqColumns = new HashSet();
        joinColumns = new HashSet();
        groupByColumns = new LinkedHashSet();
        orderByColumns = new LinkedHashSet();

        _buildIndexes(query, allColumns, eqColumns, joinColumns);
    }


    public static Multimap associateColumnsWithTables(Set columns) {
        loadColumnProperties();

        Multimap multiMap = HashMultimap.create();
        for (Iterator iterator = columns.iterator(); iterator.hasNext();) {
            String columnName = (String) iterator.next();
            String tableName = columnProperties.getProperty(columnName.toUpperCase());
            if (tableName == null) {
                System.out.println("Cannot retrieve properties for columnName = " + columnName);
                continue;
            }

            multiMap.put(tableName.toLowerCase(), columnName);
        }
        return multiMap;
    }

    public static Map GetColumnMap() throws IOException {
        Properties props = columnProperties;
        Map tableMap = new HashMap();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            String field = (String) entry.getKey();
            String table = (String) entry.getValue();

            HashSet set = (HashSet) tableMap.get(table);
            if (set == null) {
                tableMap.put(table, set = new HashSet());
            }
            set.add(field);
        }
        return tableMap;
    }


    private static void loadColumnProperties() {
    	
    	if (columnProperties == null) {
          GLOBAL_INFO.get().getConnection();
          GLOBAL_INFO.get().getTableInfo();
          columnProperties = GLOBAL_INFO.get().getProperties();
          GLOBAL_INFO.get().closeConnection();
        }
    }


	public static String getTableName(String column) {
        loadColumnProperties();
        String tableName = columnProperties.getProperty(column.toUpperCase());
        return tableName != null ? tableName.toLowerCase() : null; 
    }

    /*
       public  LinkedHashMap associateColumnsWithTables(Set columns) {
            if( columnProperties == null ) {
                try {
                    columnProperties = new Properties();
                    columnProperties.load(new FileInputStream(COLUMN_PROPERTIES));
                } catch (IOException e) {
                    assert false: "Cannot load the properties file: " + COLUMN_PROPERTIES;
                }
            }

            LinkedHashMap multiMap = new LinkedHashMap();
            for (Iterator iterator = columns.iterator(); iterator.hasNext();) {
                String columnName = (String) iterator.next();
                String tableName = columnProperties.getProperty(columnName.toUpperCase());
                if(tableName == null) {
                    continue;
                }
                LinkedHashSet list = (LinkedHashSet) multiMap.get(tableName);
                if(list == null) {
                    multiMap.put(tableName,list = new LinkedHashSet());
                }
                list.add(columnName);
            }

            return multiMap;
        }

    */


    public Set getUsedColumns() {

        HashSet caps = new HashSet();
        for (Iterator ci = allColumns.iterator(); ci.hasNext();) {
            caps.add(((String) ci.next()).toUpperCase());
        }
        allColumns = caps;
        return allColumns;
    }

    public Set getWhereColumns() {
        return toUpperCaseSet(whereColumns);
    }

    public Set getInterestingOrders() {
        HashSet caps = new HashSet();

        for (Iterator ci = joinColumns.iterator(); ci.hasNext();) {
            caps.add(((String) ci.next()).toUpperCase());
        }
        if(!groupByColumns.isEmpty())
            caps.add(getGroupByColumns().iterator().next());

        if(!orderByColumns.isEmpty())
            caps.add(getOrderByColumns().iterator().next());

        return caps;
    }

    public Set getGroupByColumns() {
        LinkedHashSet caps = new LinkedHashSet();
        for (Iterator ci = groupByColumns.iterator(); ci.hasNext();) {
            caps.add(((String) ci.next()).toUpperCase());
        }
        groupByColumns = caps;
        return groupByColumns;
    }

    public Set getOrderByColumns() {
        LinkedHashSet caps = toUpperCaseSet(orderByColumns);
        orderByColumns = caps;
        return orderByColumns;
    }

    private LinkedHashSet toUpperCaseSet(Set orderByColumns) {
        LinkedHashSet caps = new LinkedHashSet();
        for (Iterator ci = orderByColumns.iterator(); ci.hasNext();) {
            caps.add(((String) ci.next()).toUpperCase());
        }
        return caps;
    }

    public Set getIndexableColumns() {
        return toUpperCaseSet(eqColumns);
    }

    private void _buildIndexes(ZQuery query, Set allColumns, Set indexableColumns, Set joinColumns) {
        processSelectClause(query.getSelect(), allColumns);
        whereColumns = new HashSet();
        _processWhereClause(query.getWhere(), whereColumns, indexableColumns, joinColumns);
        allColumns.addAll(whereColumns);
        _processGrouByClause(query.getGroupBy(), allColumns);
        _processOrderByClause(query.getOrderBy(), allColumns);
    }

    private void _processOrderByClause(Vector orderBy, Set allColumns) {
        if (orderBy == null) {
            return;
        }

        Set orderByColumn = new LinkedHashSet();
        for (Iterator iterator = orderBy.iterator(); iterator.hasNext();) {
            ZOrderBy by = (ZOrderBy) iterator.next();
            _processExpression(by.getExpression(), allColumns, allColumns, orderByColumn);
        }
        allColumns.addAll(orderByColumn);

        orderByColumn = new LinkedHashSet();
        for (Iterator iterator = orderBy.iterator(); iterator.hasNext();) {
            ZOrderBy by = (ZOrderBy) iterator.next();
            if(by.getExpression() instanceof ZConstant) {
                _processExpression(by.getExpression(), allColumns, allColumns, orderByColumn);                
            }
        }
        orderByColumns.addAll(orderByColumn);
    }

    private void _processGrouByClause(ZGroupBy groupBy, Set allColumns) {
        if (groupBy == null) {
            return;
        }

        Set groupByColumn = new LinkedHashSet();
        for (Iterator iterator = groupBy.getGroupBy().iterator(); iterator.hasNext();) {
            ZExp o = (ZExp) iterator.next();
            if(o instanceof ZConstant) {
                _processExpression(o, allColumns, allColumns, groupByColumn);                
            }
        }
        groupByColumns.addAll(groupByColumn);

        groupByColumn = new LinkedHashSet();
        for (Iterator iterator = groupBy.getGroupBy().iterator(); iterator.hasNext();) {
            ZExp o = (ZExp) iterator.next();
            _processExpression(o, groupByColumn, allColumns, groupByColumn);
        }
        eqColumns.addAll(groupByColumn);        

        // 	for (Iterator gi = groupByColumn.iterator(); gi.hasNext();) {
        // 	    groupByColumns.add(gi.next());
        // 	}
    }


    public Multimap getOrderByOrders() {

        LinkedHashSet orderByColumns = (LinkedHashSet) getOrderByColumns();
        // System.out.println("getOrderByOrders: orderByColumns = " + orderByColumns);


        if (orderByColumns.size() == 0) {
            return null;
        }
        Multimap orderByConfig = associateColumnsWithTables(orderByColumns);

        return orderByConfig;
    }


    public Multimap getGroupByOrders() {
        LinkedHashSet groupByColumns = (LinkedHashSet) getGroupByColumns();

        if (groupByColumns.size() == 0) {
            return null;
        }
        Multimap groupByConfig = associateColumnsWithTables(groupByColumns);
        /*
          if (groupByColumns.size() == 0) {
              return null;
          }
          Configuration groupByConfig = associateColumnsWithTables(groupByColumns);
          Configuration result = new Configuration();
          ArrayList tableNames = groupByConfig.getIndexedTableNames();
          String tableName = (String) tableNames.get(0);
          result.addColumnSet(tableName, groupByConfig.getSingleTableSet(tableName));
          return result;
      */
        return groupByConfig;
    }

    private void processSelectClause(Vector select, Set allColumns) {
        if (select == null) {
            return;
        }

        Set selectColumns = new HashSet();
        for (Iterator iterator = select.iterator(); iterator.hasNext();) {
            ZSelectItem s = (ZSelectItem) iterator.next();
            _processExpression(s.getExpression(), allColumns, selectColumns, selectColumns);
        }
    }

    private void _processWhereClause(ZExp where, Set allColumns, Set eqColumns, Set joinColumns) {
        if (where instanceof ZExpression) {
            ZExpression expression = (ZExpression) where;
            String oper = expression.getOperator();

            if (ZqlUtils.AGGR_OPER.contains(oper)) {
                for (Iterator iterator = expression.getOperands().iterator(); iterator.hasNext();) {
                    ZExp exp = (ZExp) iterator.next();

                    _processWhereClause(exp, allColumns, eqColumns, joinColumns);
                }
            } else {

                // System.out.println("oper = " + oper + ", numOperands = " + expression.nbOperands());
                // this is a basic operator.
                Set newColumns = new HashSet();
                Set newJoinColumns = new HashSet();

                int columnCount = 0;
                boolean notSargable = false;
                for (Iterator iterator = expression.getOperands().iterator(); iterator.hasNext();) {
                    ZExp exp = (ZExp) iterator.next();

                    Set cols = new HashSet();

                    if (exp instanceof ZConstant) {
                        ZConstant constant1 = (ZConstant) exp;
                        if (ZConstant.COLUMNNAME == constant1.getType()) {
                            String value = constant1.getValue();
                            int tblnameIdx = value.lastIndexOf('.');

                            String col = value.substring(tblnameIdx + 1);
                            newColumns.add(col);

                            columnCount ++;
                        }
                    } else if (exp instanceof ZExpression) {
                        ZExpression zExp = (ZExpression) exp;
                        _processWhereClause(zExp, cols, newColumns, newJoinColumns);
                        if(!cols.isEmpty()) {
                            notSargable = true;
                        }
                    } else if (exp instanceof ZQuery) {
                        ZQuery zQuery = (ZQuery) exp;
                        _buildIndexes(zQuery, cols, newColumns, newJoinColumns);
                    } else {
                        assert false: "Invalid operand type: " + exp.getClass().getName();
                    }

                    allColumns.addAll(cols);
                    allColumns.addAll(newColumns);
                    allColumns.addAll(newJoinColumns);
                }

                if (columnCount == 1) {
                    if(!notSargable)
                        eqColumns.addAll(newColumns);
                } else
                // yet another hack in the bag of hacks.
                if (columnCount == 2 && !(oper.equals("datepart") || oper.equals("substring"))) {
                    //and columns do not belong to the same table ...
                    ArrayList columnList = new ArrayList(newColumns);

                    if (columnList.size() <= 1) {
                        return;
                    }

                    int dashIdx = ((String) columnList.get(0)).lastIndexOf('_');
                    String table1 = ((String) columnList.get(0)).substring(0, dashIdx + 1);

                    //System.out.println("processWhereClause: " + newColumns + " count " + columnCount);
                    dashIdx = ((String) columnList.get(1)).lastIndexOf('_');
                    String table2 = ((String) columnList.get(1)).substring(0, dashIdx + 1);


                    if (table1.compareTo(table2) != 0)
                        joinColumns.addAll(newColumns);
                }

                joinColumns.addAll(newJoinColumns);
            }
        }
    }

    private void _processExpression(ZExp exp, Set allColumns, Set eqColumns, Set joinColumns) {
        if (exp instanceof ZConstant) {
            ZConstant constant = (ZConstant) exp;
            if (ZConstant.COLUMNNAME == constant.getType()) {
                String value = constant.getValue();
                int tblnameIdx = value.lastIndexOf('.');

                String col = value.substring(tblnameIdx + 1);
                eqColumns.add(col);
                allColumns.add(col);
            }
        } else if (exp instanceof ZExpression) {
            ZExpression zExp = (ZExpression) exp;
            _processWhereClause(zExp, allColumns, eqColumns, joinColumns);
        } else if (exp instanceof ZQuery) {
            ZQuery zQuery = (ZQuery) exp;
            _buildIndexes(zQuery, allColumns, eqColumns, joinColumns);
        } else {
            assert false: "Invalid operand type: " + exp.getClass().getName();
        }
    }

    public Set getQueryTables() {
        Set set = new HashSet();
        Vector v = this.query.getFrom();
        for (int i = 0; i < v.size(); i++) {
            ZFromItem item = (ZFromItem) v.get(i);
            set.add(item.getTable().toLowerCase());
        }

        return set;
    }
}
