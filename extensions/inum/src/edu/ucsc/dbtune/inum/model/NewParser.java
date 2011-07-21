package edu.ucsc.dbtune.inum.model;

import com.adventnet.swissqlapi.SwisSQLAPI;
import com.adventnet.swissqlapi.config.metadata.MetaDataProperties;
import com.adventnet.swissqlapi.sql.exception.ConvertException;
import com.adventnet.swissqlapi.sql.functions.FunctionCalls;
import com.adventnet.swissqlapi.sql.parser.ParseException;
import com.adventnet.swissqlapi.sql.statement.SwisSQLStatement;
import com.adventnet.swissqlapi.sql.statement.select.FromTable;
import com.adventnet.swissqlapi.sql.statement.select.OrderItem;
import com.adventnet.swissqlapi.sql.statement.select.SelectColumn;
import com.adventnet.swissqlapi.sql.statement.select.SelectQueryStatement;
import com.adventnet.swissqlapi.sql.statement.select.TableColumn;
import com.adventnet.swissqlapi.sql.statement.select.WhereItem;
import com.thoughtworks.xstream.XStream;
import edu.ucsc.dbtune.inum.Config;
import edu.ucsc.dbtune.inum.autopilot.autopilot;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.Vector;

/**
 * Created by IntelliJ IDEA.
 * User: dash
 * Date: Apr 7, 2009
 * Time: 12:29:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class NewParser {
    public SwisSQLAPI parser;

    public NewParser() {
        // load the file information, etc.
        parser = new SwisSQLAPI();
        File metadataFile = Config.getMetadataFile();
        if (metadataFile.exists()) {
            parser.loadMetaData(metadataFile.getAbsolutePath());
            HashMap map = parser.getObjectNames();
            //System.out.println("map = " + map);
            //System.out.println("SwisSQLAPI.dataTypesFromMetaDataHT = " + SwisSQLAPI.dataTypesFromMetaDataHT);
            //System.out.println("SwisSQLAPI.tableColumnListMetadata = " + SwisSQLAPI.tableColumnListMetadata);
        } else {
            autopilot ap;
            ap = new autopilot();
            ap.init_database();

            MetaDataProperties property = new MetaDataProperties();
            property.setCatalogName("*");
            property.setColumnNamePattern("*");
            property.setTableNamePattern("*");
            property.setSchemaName("*");
            property.setMetadataStorageFile(metadataFile.getAbsolutePath());
            try {
                Connection connection = ap.getConnection();
                parser.getMetaData(connection, property);
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void parseSQL(String str) throws ConvertException, ParseException, IOException {
        //parser.loadMetaData();
        // call to convert() api.
        parser.setSQLString(str);
        SwisSQLStatement statement = parser.parse();
        XStream xstream = new XStream();
        FileOutputStream fos = new FileOutputStream("query.xml");
        xstream.toXML(statement, fos);
        fos.close();

        if (statement instanceof SelectQueryStatement) {
            SelectQueryStatement sstatement = (SelectQueryStatement) statement;
            traverse(new QueryColumnInfo(), new Context(), sstatement);
        }
    }

    public void traverse(QueryColumnInfo info, Context context, SelectQueryStatement select) {
        traverseFrom(info, context, select.getFromClause().getFromItemList());
        traverseItemList(info, context.addContext(Context.Type.SELECT), select.getSelectStatement().getSelectItemList());
        traverseItemList(info, context.addContext(Context.Type.WHERE), select.getWhereExpression().getWhereItems());
        if(select.getGroupByStatement() != null)
            traverseItemList(info, context.addContext(Context.Type.ORDER), select.getGroupByStatement().getGroupByItemList());
        if(select.getOrderByStatement() != null)
            traverseItemList(info, context.addContext(Context.Type.ORDER), select.getOrderByStatement().getOrderItemList());

        System.out.println("info = " + info);
    }

    private void traverseItemList(QueryColumnInfo info, Context context, Vector itemList) {
        if(itemList == null) return;
        
        for (Object colExp : itemList) {
            if (colExp instanceof TableColumn) {
                TableColumn tCol = (TableColumn) colExp;
                String tName = getTableNameFromColumn(tCol.getColumnName());
                ColumnInfo cInfo = new ColumnInfo(null, null, tCol.getColumnName());
                if (tName != null) {
                    tCol.setOrigTableName(tName);
                }
                info.allColumns.add(cInfo);
                switch (context.getCurrentContext()) {
                    case COND:
                        info.conditionColumns.add(cInfo);
                        break;
                    case JOIN:
                        info.joinColumns.add(cInfo);
                        break;
                    case ORDER:
                        info.orderColumns.add(cInfo);
                        break;
                }
            } else if (colExp instanceof SelectColumn) {
                SelectColumn selColumn = (SelectColumn) colExp;
                traverseItemList(info, context, selColumn.getColumnExpression());
            } else if (colExp instanceof FunctionCalls) {
                FunctionCalls calls = (FunctionCalls) colExp;
                Vector arguments = calls.getFunctionArguments();
                traverseItemList(info, context.addContext(Context.Type.FUNC), arguments);
            } else if (colExp instanceof OrderItem) {
                OrderItem oItem = (OrderItem) colExp;
                traverseItemList(info, context.addContext(Context.Type.ORDER), oItem.getOrderSpecifier().getColumnExpression());
            } else if (colExp instanceof WhereItem) {
                WhereItem item = (WhereItem) colExp;
                //Vector whereItems = exp.getWhereItems();
                //for (WhereItem item : (Vector<WhereItem>) whereItems) {
                Context newContext = context.addContext(item.isItAJoinItem() ? Context.Type.JOIN : Context.Type.COND);
                traverseItemList(info, newContext, item.getLeftWhereExp().getColumnExpression());
                if(item.getRightWhereExp() != null) {
                    traverseItemList(info, newContext, item.getRightWhereExp().getColumnExpression());
                } else if( item.getRightWhereSubQuery() != null ){
                    traverse(info, newContext.addContext(Context.Type.SUB_QUERY), item.getRightWhereSubQuery());
                }
            } else if (colExp instanceof String) {
                // do nothing.
            } else {
                throw new UnsupportedOperationException("Invalid type: " + colExp.getClass() + ", with value :" + colExp);
            }
        }
    }

    private void traverseFrom(QueryColumnInfo info, Context context, Vector fromItemList) {
        for (int i = 0; i < fromItemList.size(); i++) {
            FromTable table = (FromTable) fromItemList.get(i);
            if (table.getTableName() instanceof String) {
                String tableName = (String) table.getTableName();
                info.tables.add(new TableInfo(tableName, table.getAliasName()));
            } else {
                System.err.println("Unable to handle the subselects now");
            }
        }
    }

    private String getTableNameFromColumn(String columnName) {
        columnName = columnName.toUpperCase();

        for (Iterator<Map.Entry> iterator = SwisSQLAPI.tableColumnListMetadata.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry entry = iterator.next();
            String key = (String) entry.getKey();
            List columns = (List) entry.getValue();

            if (columns.contains(columnName)) {
                return key.toLowerCase();
            }
        }

        return null;
    }

    public void parseSQL(File filename) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String query = "";
        String line = null;
        while ((line = reader.readLine()) != null) {
            line = line.replaceAll("--.*", ""); // remove the comments.
            query += " " + line;
            if (query.endsWith(";")) {
                try {
                    parseSQL(query.substring(0, query.length() - 1));
                } catch (Throwable e) {
                    System.err.println(query);
                    //e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    System.err.println(e);
                }
                query = "";
            }
        }
    }

    public static class Context {
        static enum Type {
            NONE,
            SELECT,
            COND,
            SUB_QUERY,
            FUNC, JOIN, ORDER, WHERE;
        }

        public Context() {
            this.contexts = new Stack();
        }
        private Context(Stack context) {
            this.contexts = context;
        }

        Stack<Type> contexts = new Stack<Type>();

        public Context addContext(Type select) {
            contexts.push(select);
            return this;
        }

        public Context popContext() {
            Stack newStack = new Stack();
            newStack.addAll(contexts);
            newStack.pop();
            return new Context(newStack);
        }

        public Type getCurrentContext() {
            return (Type) contexts.peek();
        }

        public Stack<Type> getFullContext() {
            return contexts;
        }

    }

    interface Processor {
        public void process();
    }

    public static void main(String[] args) throws IOException {
        new NewParser().parseSQL(Config.getWorkloadFile("test-1.sql"));
    }


}