package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import Zql.ZConstant;
import Zql.ZExp;
import Zql.ZExpression;
import Zql.ZFromItem;
import Zql.ZGroupBy;
import Zql.ZOrderBy;
import Zql.ZQuery;
import Zql.ZSelectItem;
import Zql.ZUtils;
import Zql.ZqlParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.spi.Console;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.util.Strings;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Extracts the interesting orders of a query.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumInterestingOrdersExtractor implements InterestingOrdersExtractor {
  private static final Set<String> AGGR_OPER;
  private static final String      BLANK;
  private final ColumnPropertyLookup properties;

  static {
    BLANK     = "";
    Console.streaming().info("preparing the zql parser");
    AGGR_OPER = Sets.newHashSet("AND", "OR", "NOT");
    ZUtils.addCustomFunction("to_char", 2);
    ZUtils.addCustomFunction("datepart", 2);
    ZUtils.addCustomFunction("dateadd", 3);
    ZUtils.addCustomFunction("substring", 3);
    ZUtils.addCustomFunction("power", 2);
    ZUtils.addCustomFunction("round", 2);
    ZUtils.addCustomFunction("cast", 1);
    ZUtils.addCustomFunction("abs", 1);
    ZUtils.addCustomFunction("cos", 1);
    ZUtils.addCustomFunction("sqrt", 1);
    ZUtils.addCustomFunction("Log10", 1);
    ZUtils.addCustomFunction("timestamp", 1);
    ZUtils.addCustomFunction("to_date", 1);
    ZUtils.addCustomFunction("year", 1);
    ZUtils.addCustomFunction("convert", 2);
  }

  public InumInterestingOrdersExtractor(Connection connection){
    this(ColumnsProperties.INSTANCE.load(Preconditions.checkNotNull(connection)));
  }

  InumInterestingOrdersExtractor(ColumnPropertyLookup properties) {
    this.properties = properties;
  }

  @Override public Configuration extractInterestingOrders(String singleQuery) {
    @SuppressWarnings("rawtypes")
    final Vector statements   = parseQuery(singleQuery);

    final List<QueryRecord> records = Lists.newArrayList();
    for (Object statement : statements) {
      final ZQuery           each      = Objects.cast(statement, ZQuery.class);
      final QueryRecord      record    = new QueryRecord();
      final ColumnsExtractor extractor = new ColumnsExtractor(each, properties);

      // tables used by the query.
      record.usedColumns = extractor.usedColumns();
      if(!extractor.getGroupByOrders().isEmpty()) { record.groupBy = extractor.getGroupByOrders(); }
      if(!extractor.getOrderByOrders().isEmpty()) { record.orderBy = extractor.getOrderByOrders(); }

      if(record.groupBy == null) { record.groupBy = extractor.getOrderByOrders(); }

      record.interestingOrders  = extractor.interestingOrders();
      record.query              = singleQuery;

      records.add(record);
    }

    try {
        return new Configuration(interestingOrdersSpace(records, properties));
    } catch(SQLException ex) {
        throw new RuntimeException(ex);
    }
  }

  //todo(Ivo) are these types okay? They don't match to your types found in SQLTypes...
  public static String getColumnType(int x) {
    if (x == 23)        { return "integer";         }
    else if (x == 20)   { return "bigint";          }
    else if (x == 21)   { return "smallint";        }
    else if (x == 17)   { return "bytea";           }
    else if (x == 701)  { return "double precision";}
    else if (x == 1043) { return "varchar";         } // todo(Ivo) this is a bug..it should be 12
    else if (x == 1042) { return "character";       }
    else if (x == 1700) { return "numeric(8,2)";    }
    else if (x == 1082) { return "date";            }
    else if (x== 1114)  { return "timestamp";       }
    else { System.out.println("Unknown type: " + x);}
    return "";
  }

  private static List<Index> interestingOrdersSpace(List<QueryRecord> records,
      ColumnPropertyLookup properties)  throws SQLException {
    final Set<Index> indexes = Sets.newHashSet();
    int id = 0;
    //todo(Huascar) to code: if an index is already in the indexes list, then ignore it; don't store it more than once.
    for(QueryRecord each : records) {
        if(each.groupBy == null) continue;
        for (String tableName : each.groupBy.keySet()) {
            // todo(Huascar) fix table creation
            Table table = null;
            final List<Column> cols = makeColumns(table,
                    Sets.newLinkedHashSet(each.groupBy.get(tableName)), properties);
            //todo(Ivo) how can we extract this information, e.g., primary, clustered, etc.? Right now, I just gave them all false values.
            final Index index = makeIndex(cols, false, false, false);
            index.setName("sat_index_" + id);
            indexes.add(index);
            id++;
        }

        if(each.orderBy == null) continue;
        for(String tableName : each.orderBy.keySet()){
            // todo(Huascar) fix table creation
            Table table = null;
            final List<Column> cols = makeColumns(table,
                    Sets.newLinkedHashSet(each.orderBy.get(tableName)), properties);
            final Index index = makeIndex(cols, false, false, false);
            index.setName("sat_index_" + id);
            indexes.add(index);
            id++;
        }

        if(each.interestingOrders == null) continue;
        for(String tableName : each.interestingOrders.keySet()){
            // todo(Huascar) fix table creation
            Table table = null;
            final List<Column> cols = makeColumns(table,
                    Sets.newLinkedHashSet(each.interestingOrders.get(tableName)), properties);
            final Index index = makeIndex(cols, false, false, false);
            index.setName("sat_index_" + id);
            indexes.add(index);
            id++;
        }

            throw new RuntimeException("fix");
    }
    return ImmutableList.copyOf(indexes);
  }

  private static Index makeIndex(List<Column> cols, boolean primary, boolean clustered, boolean unique){
      try {
          return new Index("index", cols, primary, unique, clustered);
      } catch (Exception e) {
          throw new IllegalStateException(e);
      }
  }

  private static List<Column> makeColumns(Table table, Set<String> cols,
          ColumnPropertyLookup properties) throws SQLException {
      final Set<Column> result = Sets.newHashSet();
      for(String name : cols){
          final int type = properties.getColumnDataType(table.getName(), name);
          final Column c = new Column(table, name, type);
          result.add(c);
      }

      return Lists.newArrayList(result);
  }

  @SuppressWarnings("rawtypes")
      private static Vector parseQuery(String query) throws InumExecutionException {
          try {
              final InputStream is = new ByteArrayInputStream(query.getBytes("UTF-8"));
              final ZqlParser   p  = new ZqlParser(is);
              return p.readStatements();
          } catch (Throwable e) {
              final String errorMessage = "InumInterestingOrdersExtractor#parseQuery(..) Error. Unable to parse query.";
              Console.streaming().error(errorMessage, e);
              throw new InumExecutionException(errorMessage);
          }
      }

  /**
   * column information extractor. this class helps retrieve information of the columns from the
   * dbms.
   */
  private static class ColumnsExtractor {
      private final ZQuery      query;
      private final Set<String> allColumns;
      private final Set<String> eqColumns;
      private final Set<String> joinColumns;
      private final Set<String> groupByColumns;
      private final Set<String> orderByColumns;
      private final Set<String> whereColumns;

      private static final AtomicReference<ColumnPropertyLookup> PROPS = new AtomicReference<ColumnPropertyLookup>();

      ColumnsExtractor(ZQuery query, ColumnPropertyLookup property) {
          PROPS.set(Preconditions.checkNotNull(property));
          this.query      = query;
          allColumns      = Sets.newHashSet();
          eqColumns       = Sets.newHashSet();
          joinColumns     = Sets.newHashSet();
          groupByColumns  = Sets.newLinkedHashSet();
          orderByColumns  = Sets.newLinkedHashSet();
          whereColumns    = Sets.newHashSet();
          buildIndexes(this.query, allColumns, eqColumns, joinColumns);
      }

      public Set<String> getUsedColumns() {

          final Set<String> caps = Sets.newHashSet();
          for(String each : allColumns) {
              caps.add(each.toUpperCase());
          }

          synchronized (allColumns){
              allColumns.clear();
              allColumns.addAll(caps);
          }

          return allColumns;
      }

      public Multimap<String, String> usedColumns(){
          return associateColumnsWithTables(getUsedColumns());
      }

      @SuppressWarnings("rawtypes")
          private void processSelectClause(Vector select, Set<String> allColumns) {
              if (select == null) { return; }
              final Set<String> selectColumns = Sets.newHashSet();
              for (Object eachObj : select) {
                  final ZSelectItem eachItem = (ZSelectItem) eachObj;
                  processExpression(eachItem.getExpression(), allColumns, selectColumns, selectColumns);
              }
          }

      public Set<String> getInterestingOrders() {
          final Set<String> caps = Sets.newHashSet();

          for (String each : joinColumns)    { caps.add(each.toUpperCase());                    }
          for (String each : groupByColumns) { caps.add(each.toUpperCase());                    }
          for (String each : orderByColumns) { caps.add(each.toUpperCase());                    }

          return caps;
      }

      public Multimap<String, String> interestingOrders(){
          return associateColumnsWithTables(getInterestingOrders());
      }

      private void processExpression(ZExp exp, Set<String> allColumns, Set<String> eqColumns,
              Set<String> joinColumns) {
          if (exp instanceof ZConstant) {
              final ZConstant constant = (ZConstant) exp;
              if (ZConstant.COLUMNNAME == constant.getType()) {
                  final String value = constant.getValue();
                  final int tblnameIdx = value.lastIndexOf('.');

                  final String col = value.substring(tblnameIdx + 1);
                  eqColumns.add(col);
                  allColumns.add(col);
              }
          } else if (exp instanceof ZExpression) {
              final ZExpression zExp = (ZExpression) exp;
              processWhereClause(zExp, allColumns, eqColumns, joinColumns);
          } else if (exp instanceof ZQuery) {
              final ZQuery zQuery = (ZQuery) exp;
              buildIndexes(zQuery, allColumns, eqColumns, joinColumns);
          } else {
              assert false : "Invalid operand type: " + exp.getClass().getName();
          }
      }

      private void buildIndexes(ZQuery query, Set<String> allColumns, Set<String> eqColumns,
              Set<String> joinColumns) {

          processSelectClause(query.getSelect(), allColumns);
          if(!whereColumns.isEmpty()){
              synchronized (whereColumns){
                  whereColumns.clear();
              }
          }

          processWhereClause(query.getWhere(), whereColumns, eqColumns, joinColumns);
          allColumns.addAll(whereColumns);
          processGrouByClause(query.getGroupBy(), allColumns);
          processOrderByClause(query.getOrderBy(), allColumns);
      }

      private void processOrderByClause(@SuppressWarnings("rawtypes") Vector orderBy, Set<String> allColumns) {
          if (orderBy == null) { return; }

          Set<String> orderByColumn = Sets.newLinkedHashSet();
          for (Object eachOB : orderBy) {
              ZOrderBy zOrderBy = (ZOrderBy) eachOB;
              processExpression(zOrderBy.getExpression(), allColumns, orderByColumn, allColumns);
          }

          allColumns.addAll(orderByColumn);
          orderByColumns.addAll(orderByColumn);
      }

      private void processGrouByClause(ZGroupBy groupBy, Set<String> allColumns) {
          if (groupBy == null) {
              return;
          }

          Set<String> groupByColumn = Sets.newLinkedHashSet();
          for (Object eachGroupBy : groupBy.getGroupBy()) {
              final ZExp o = (ZExp) eachGroupBy;
              if (o instanceof ZConstant) {
                  processExpression(o, allColumns, allColumns, groupByColumn);
              }
          }

          groupByColumns.addAll(groupByColumn);
          groupByColumn = Sets.newLinkedHashSet();

          // todo(huascar) not sure why this has to be done twice (see above loop)....there must be something required by ZQL library.
          for (Object eachGroupBy : groupBy.getGroupBy()) {
              final ZExp o = (ZExp) eachGroupBy;
              processExpression(o, groupByColumn, allColumns, groupByColumn);
          }

          eqColumns.addAll(groupByColumn);
      }

      public Multimap<String, String> getOrderByOrders() {
          final Set<String> orderByColumns = getOrderByColumns();
          if (orderByColumns.isEmpty()) {
              return HashMultimap.create();
          }

          return associateColumnsWithTables(orderByColumns);
      }

      public static Multimap<String, String> associateColumnsWithTables(Set<String> columns) {
          final Multimap<String, String> multiMap = HashMultimap.create();
          for (String columnName : columns) {
              String tableName = PROPS.get().getProperty(columnName.toUpperCase());
              if (tableName == null) {
                  Console.streaming().info("Cannot retrieve properties for columnName = " + columnName);
                  continue;
              }

              multiMap.put(tableName.toLowerCase(), columnName);
          }

          return multiMap;
      }

      public Set<String> getOrderByColumns() {
          final Set<String> caps = toUpperCaseSet(orderByColumns);
          synchronized (orderByColumns) {
              orderByColumns.clear();
              orderByColumns.addAll(caps);
          }
          return ImmutableSet.copyOf(orderByColumns);
      }

      private Set<String> toUpperCaseSet(Set<String> orderByColumns) {
          final Set<String> caps = Sets.newLinkedHashSet();
          for (String orderByColumn : orderByColumns) {
              caps.add((orderByColumn).toUpperCase());
          }
          return caps;
      }

      public Multimap<String, String> getGroupByOrders() {
          final Set<String> groupByColumns = getGroupByColumns();
          if (groupByColumns.isEmpty()) {
              return HashMultimap.create();
          }

          return associateColumnsWithTables(groupByColumns);
      }

      public Set<String> getIndexableColumns() {
          return toUpperCaseSet(eqColumns);
      }

      public Set<String> getGroupByColumns() {
          final Set<String> caps = Sets.newLinkedHashSet();
          for (String each : groupByColumns) {
              caps.add(each.toUpperCase());
          }
          synchronized (groupByColumns) {
              groupByColumns.clear();
              groupByColumns.addAll(caps);
          }

          return groupByColumns;
      }

      public Set<String> getQueryTables() {
          final Set<String> tableNames = Sets.newHashSet();
          @SuppressWarnings("rawtypes")
              Vector from = this.query.getFrom();
          for (Object each : from) {
              ZFromItem item = (ZFromItem) each;
              tableNames.add(item.getTable().toLowerCase());
          }

          return tableNames;
      }

      private void processWhereClause(ZExp where, Set<String> allColumns,
              Set<String> eqColumns, Set<String> joinColumns) {
          if (where instanceof ZExpression) {
              final ZExpression expression  = (ZExpression) where;
              final String      oper        = expression.getOperator();

              if (AGGR_OPER.contains(oper)) {
                  for (Object o : expression.getOperands()) {
                      ZExp eachExpression = (ZExp) o;
                      processWhereClause(eachExpression, allColumns, eqColumns, joinColumns);
                  }
              } else {
                  processBasicOperator(expression, oper, allColumns, eqColumns, joinColumns);
              }
          }
      }

      private void processBasicOperator(ZExpression expression, String oper,
              Set<String> allColumns, Set<String> eqColumns, Set<String> joinColumns) {
          // this is a basic operator.
          Set<String> newColumns     = Sets.newHashSet();
          Set<String> newJoinColumns = Sets.newHashSet();

          int columnCount     = 0;
          boolean notSargable = false;
          for (Object o : expression.getOperands()) {
              final ZExp exp   = (ZExp) o;
              final Set<String> cols  = Sets.newHashSet();

              if (exp instanceof ZConstant) {
                  ZConstant constant1 = (ZConstant) exp;
                  if (ZConstant.COLUMNNAME == constant1.getType()) {
                      String value = constant1.getValue();
                      int tblnameIdx = value.lastIndexOf('.');

                      String col = value.substring(tblnameIdx + 1);
                      newColumns.add(col);

                      columnCount++;
                  }
              } else if (exp instanceof ZExpression) {
                  ZExpression zExp = (ZExpression) exp;
                  processWhereClause(zExp, cols, newColumns, newJoinColumns);
                  if (!cols.isEmpty()) {
                      notSargable = true;
                  }
              } else if (exp instanceof ZQuery) {
                  ZQuery zQuery = (ZQuery) exp;
                  buildIndexes(zQuery, cols, newColumns, newJoinColumns);
              } else {
                  assert false : "Invalid operand type: " + exp.getClass().getName();
              }

              allColumns.addAll(cols);
              allColumns.addAll(newColumns);
              allColumns.addAll(newJoinColumns);
          }

          if (columnCount == 1) {
              if (!notSargable) { eqColumns.addAll(newColumns); }
          } else
              // yet another hack in the bag of hacks.
              if (columnCount == 2 && !(oper.equals("datepart") || oper.equals("substring"))) {
                  //and columns do not belong to the same table ...
                  final List<String> columnList = Lists.newArrayList(newColumns);

                  if (columnList.size() <= 1) {
                      return;
                  }

                  int dashIdx   = columnList.get(0).lastIndexOf('_');
                  String table1 = columnList.get(0).substring(0, dashIdx + 1);

                  dashIdx = columnList.get(1).lastIndexOf('_');
                  String table2 = columnList.get(1).substring(0, dashIdx + 1);

                  if (table1.compareTo(table2) != 0) { joinColumns.addAll(newColumns); }
              }

          joinColumns.addAll(newJoinColumns);
      }
  }

  /**
   * A record object that holds information of columns.
   */
  public static class ColumnInformation {
      String  columnName;
      String  columnType;

      int     attnum;
      int     atttypid;

      boolean isNullable;

  }
  // todo(Huascar) convert this to interface.
  public enum ColumnsProperties implements ColumnPropertyLookup {
      INSTANCE;

      final Map<String, Set<ColumnInformation>> stringToSetOfColumnInfo;
      Connection                        connection;
      Properties                                properties;

      ColumnsProperties(){
          stringToSetOfColumnInfo   = Maps.newConcurrentMap();
          properties                = new Properties();
      }

      public Connection getDatabaseConnection() {
          try {
              if(connection.isClosed()) throw new IllegalStateException("Connection is closed.");
          } catch(SQLException ex) {
              throw new RuntimeException(ex);
          }

          return connection;
      }

      ColumnsProperties load(Connection connection){
          this.connection = Preconditions.checkNotNull(connection);
          refresh();
          properties      = getProperties();
          return this;
      }

      @Override public String getProperty(String key){
          final String property = properties.getProperty(key);
          return property == null ? BLANK : property;
      }

      @Override public void refresh() {
          final String sqlquery
              = "select relname,relpages, reltuples, relfilenode from pg_class where relnamespace = 2200 and relam =0";
          try {
              final Statement statement = connection.createStatement();
              final ResultSet resultSet = statement.executeQuery(sqlquery);
              String  tbName;
              int     reloid;
              while (resultSet.next()){
                  tbName    = resultSet.getString(1);
                  reloid    = resultSet.getInt(4);

                  final Set<ColumnInformation> columnInformation = getColumnInformation(reloid);
                  stringToSetOfColumnInfo.put(tbName, columnInformation);
              }
          } catch (Exception error) {
              Console.streaming().error("Unable to execute query=" + sqlquery, error);
          }
      }

      @Override public Properties getProperties() {
          final Properties props = new Properties();
          for(String eachTableName : stringToSetOfColumnInfo.keySet()){
              final Set<ColumnInformation> info = stringToSetOfColumnInfo.get(eachTableName);
              for(ColumnInformation eachColumnInfo : info){
                  props.setProperty(eachColumnInfo.columnName.toUpperCase(), eachTableName);
              }
          }
          return props;
      }

      @Override public Set<ColumnInformation> getColumnInformation(int reloid) {
          final Set<ColumnInformation> columns = Sets.newHashSet();
          final String sqlquery =
              "select attname, attnum, atttypid, attnotnull from pg_attribute where attnum > 0 and attrelid = "
              + reloid;
          try {
              final Statement statement = connection.createStatement();
              final ResultSet resultSet = statement.executeQuery(sqlquery);
              int columnType;
              while (resultSet.next()){
                  final ColumnInformation info = new ColumnInformation();
                  info.columnName = resultSet.getString(1);
                  info.attnum     = resultSet.getInt(2);
                  columnType      = resultSet.getInt(3);

                  final String tip      = getColumnType(columnType);
                  final String nullable = resultSet.getString(4);

                  info.isNullable = nullable.compareTo("f") == 0;
                  info.atttypid   = columnType;
                  info.columnType = tip;

                  columns.add(info);
              }

          } catch (Exception error){
              Console.streaming().error("unable to execute query=" + sqlquery, error);
          }
          return columns;
      }

      // todo(Huascar) this is slow...must be improved.
      @Override public int getColumnDataType(String tableName, String columnName) {
          for(ColumnInformation each : stringToSetOfColumnInfo.get(tableName)) {
              if(Strings.same(each.columnName, columnName)) return each.atttypid;
          }
          return -1;
      }
  }

  /**
   * A record object that holds information of a single query.
   */
  public static class QueryRecord {
      Multimap<String, String> usedColumns;
      Multimap<String, String> interestingOrders;
      Multimap<String, String> groupBy;
      Multimap<String, String> orderBy;
      String                   query;
  }

}
