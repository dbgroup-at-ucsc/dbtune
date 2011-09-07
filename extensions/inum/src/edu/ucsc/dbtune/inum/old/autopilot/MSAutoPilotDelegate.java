package edu.ucsc.dbtune.inum.old.autopilot;

import Zql.ZConstant;
import Zql.ZFromItem;
import Zql.ZQuery;
import Zql.ZSelectItem;

import edu.ucsc.dbtune.inum.old.commons.Cloner;
import edu.ucsc.dbtune.inum.old.commons.ZqlUtils;
import edu.ucsc.dbtune.inum.old.model.ColumnsGatherer;
import edu.ucsc.dbtune.inum.old.model.Configuration;
import edu.ucsc.dbtune.inum.old.model.Index;
import edu.ucsc.dbtune.inum.old.model.MSPlan;
import edu.ucsc.dbtune.inum.old.model.MatView;
import edu.ucsc.dbtune.inum.old.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.old.model.Plan;
import edu.ucsc.dbtune.inum.old.model.QueryDesc;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Dec 10, 2007
 * Time: 7:13:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class MSAutoPilotDelegate extends AutoPilotDelegate {
    private int count;
    private MSGlobalInfo globalInfo;
    private static ZSelectItem COUNT_BIG = null;

    public static final String MS_CONNECTION_INIT =
            "IF sessionproperty('ARITHABORT') = 0 SET ARITHABORT ON\n" +
                    "IF sessionproperty('CONCAT_NULL_YIELDS_NULL') = 0 SET CONCAT_NULL_YIELDS_NULL ON\n" +
                    "IF sessionproperty('QUOTED_IDENTIFIER') = 0 SET QUOTED_IDENTIFIER ON\n" +
                    "IF sessionproperty('ANSI_NULLS') = 0 SET ANSI_NULLS ON\n" +
                    "IF sessionproperty('ANSI_PADDING') = 0 SET ANSI_PADDING ON\n" +
                    "IF sessionproperty('ANSI_WARNINGS') = 0 SET ANSI_WARNINGS ON\n" +
                    "IF sessionproperty('NUMERIC_ROUNDABORT') = 1 SET NUMERIC_ROUNDABORT OFF";
    private static final int MAX_VIEW_ROWCOUNT = 10000000;

    public Plan getExecutionPlan(Connection conn, String Q) throws SQLException {
        MSPlan plan = new MSPlan();
        Statement stmt = conn.createStatement();
        stmt.execute(MS_CONNECTION_INIT);
        stmt.execute("set showplan_all on");

        try {
            stmt.execute(Q);
        } catch (SQLException e) {
            System.err.println("SQL: " + Q);
            throw e;
        }

        ResultSet rs = stmt.getResultSet();

        while (rs.next()) {
            MSPlanDesc PD = new MSPlanDesc(rs.getInt("NodeId"), rs.getInt("Parent"), rs.getString("PhysicalOp"), rs.getString("Argument"), rs.getFloat("EstimateRows"), rs.getFloat("EstimateIO"), rs.getFloat("EstimateCPU"), rs.getFloat("TotalSubtreeCost"));
            //System.out.println("\t optimizer_cost: " + PD.physicalOp);
            plan.add(PD);
        }

        stmt.execute("set showplan_all off");
        rs.close();
        stmt.close();

        plan.analyzePlan();

        return plan;
    }

    public void implement_configuration(autopilot ap, PhysicalConfiguration configuration, Connection conn) {
        int total_size = 0;

        if (configuration.isEmpty())
            return;

        //System.out.println("implement_configuration: config = " + configuration);

        try {
            Map implementedIndexes = new HashMap();
            Map indexMap = new HashMap();

            Statement helperStatement = conn.createStatement();
            String helperQ = "set showplan_all off;";
            helperStatement.execute(helperQ);
            helperQ = "update sysindexes set status = 0 where name like '_idx_%';";
            helperStatement.execute(helperQ);

            implement_indexes(configuration, indexMap, conn);
            implement_mat_views(configuration, indexMap, conn);

            if (indexMap.isEmpty()) return;

            StringBuffer buffer = new StringBuffer();
            for (Iterator iterator = indexMap.keySet().iterator(); iterator.hasNext();) {
                String str = (String) iterator.next();
                if (buffer.length() != 0) {
                    buffer.append(", ");
                }
                buffer.append('\'').append(str).append('\'');
            }

            Statement catalogStmt = conn.createStatement();
            //now check the id, indid combinations ...
            String Qcatalog = "select id, indid, name from sysindexes where name in (" + buffer.toString() + ") order by name";
            System.out.println("Qcatalog = " + Qcatalog);
            catalogStmt.execute(Qcatalog);
            ResultSet rs = catalogStmt.getResultSet();
            Statement updateStatement = conn.createStatement();
            String Qupdate = new String();
            while (rs.next()) {
                int idi[] = (int[]) indexMap.get(rs.getString(3));
                if (idi == null) {
                    continue;
                }
                // for view
                if (idi[2] == 1) {
                    /*
                       update sysindexes set root=0x0, first = 0x0, statblob = 0x0, firstiam = 0x0, status = 2113554, dpages = 16, rowcnt = 574, reserved = 24, used = 18, indid = 1  where id = 846066200 and indid = 1
                           update sysobjects set status = 1677721601 where id = 846066200

                        */
                    int oID = rs.getInt("id");
                    Qupdate += "update sysindexes set statblob = 0x0, status=2113554, rowcnt=" + idi[0] + ", dpages=" + idi[1] + ", reserved = 24, used = 18, indid = 1 where id = " + oID + " and indid= " + rs.getInt("indid") + "\n";
                    Qupdate += "update sysobjects set status = 1677721601 where id = " + oID + "\n";
                } else {
                    Qupdate += "update sysindexes set statblob = 0x0, status=0, rowcnt=" + idi[0] + ", dpages=" + idi[1] + " where id = " + rs.getInt("id") + " and indid= " + rs.getInt("indid") + "\n";
                }
            }

            System.out.println("implement_configuration: " + Qupdate);
            updateStatement.execute(Qupdate);

            //disable the helper indexes
            helperStatement.execute("update sysindexes set status = 10485792 where name like '_idx_%'");
            helperStatement.close();


            catalogStmt.close();
            updateStatement.close();
        }
        catch (Exception E) {
            System.out.println("implement_configuration: " + E.getMessage());
            E.printStackTrace();
        }
    }

    private void implement_indexes(PhysicalConfiguration configuration, Map indexMap, Connection conn) throws SQLException {
        String Q = new String();
        for (String tableName : configuration.getIndexedTableNames()) {
            table_info ti = (table_info) globalInfo.global_table_info.get(tableName);
            int row_cnt = ti.row_count;
            //for each ColumnSet in the given TableSet
            for (Index idx : configuration.getIndexesForTable(tableName)) {
                //obtain the attributes ..
                LinkedHashSet theColumnSet = idx.getColumns();
                int count;
                synchronized (this) {
                    count = this.count++;
                }
                String indexName = "idxdesigner_" + tableName + "_" + (100 + count);
                String[] attributes = (String[]) theColumnSet.toArray(new String[theColumnSet.size()]);

                ArrayList index_col_info = new ArrayList();
                for (int ai = 0; ai < attributes.length; ai++) {
                    index_col_info.add(ti.col_info.get(attributes[ai]));
                }
                int dpages = index_size(ti, index_col_info);
                indexMap.put(indexName, new int[]{row_cnt, dpages, 0});
                // System.out.println("autopilot: dpages " + dpages + " rowcnt " + row_cnt);

                //implement the index and obtain its catalog entry ...
                Q += "create index " + indexName + " on " + tableName + " (";
                for (int ai = 0; ai < attributes.length; ai++) {
                    Q += attributes[ai];
                    if (ai < (attributes.length - 1)) Q += ",";
                }
                Q += " ) with statistics_only\n";

                idx.setImplementedName(indexName);
            }
        }

        if (Q.length() > 0) {
            Statement stmt = conn.createStatement();
            System.out.println("implement_configuration: " + Q);
            stmt.execute(Q);
            stmt.close();
        }
    }

    public void implement_mat_views(PhysicalConfiguration config, Map indexMap, Connection conn) throws SQLException {
        String Q = "";
        Statement stmt = conn.createStatement();
        stmt.execute(MS_CONNECTION_INIT);

        for (Iterator iter = config.getMaterializedViews().iterator(); iter.hasNext();) {
            MatView view = (MatView) iter.next();
            ZQuery query = Cloner.clone(view.getQuery());

            ZQuery zQuery = ZqlUtils.addSchemaToTables(query, "dbo");
            zQuery = removeInvalidSelects(zQuery);
            if (zQuery.getSelect().isEmpty()) continue;

            Vector v = zQuery.getSelect();
            Set<String> aliases = new HashSet();
            for (int i = 0; i < v.size(); i++) {
                ZSelectItem item = (ZSelectItem) v.elementAt(i);
                if (item.getExpression() instanceof ZConstant) {
                    aliases.add(item.getColumn());
                } else {
                    if (item.isExpression() && item.getAlias() == null) {
                        item.setAlias("column" + i);
                    }
                    aliases.add(item.getAlias());
                }
            }

            String qString = null;
            try {
                qString = zQuery.toString();
            } catch (ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
                throw e;
            }
            if (zQuery.getGroupBy() != null) {
                qString = qString.replaceFirst("from", ", COUNT_BIG(*) as count_big from");
            }
            Plan plan = getExecutionPlan(conn, qString);
            int rowCount = (int) ((MSPlanDesc) plan.getRoot()).estimateRows;
            float cost = plan.getTotalCost();

            synchronized (this) {
                count++;
            }

            // first create the view.
            String viewName = "idxdesigner_mv" + (100 + count);
            String vSQL = "create view " + viewName + " with schemabinding as " + qString;
            try {
                System.out.println("vSQL = " + vSQL);
                stmt.execute(vSQL);
            } catch (SQLException e) {
                System.err.println("SQL = " + vSQL);
                throw e;
            }

            view.setImplementedName(viewName);

            ArrayList index_col_info = new ArrayList();
            table_info info = globalInfo.get_table_info(conn, viewName, rowCount, 0);

            index_col_info.addAll(info.col_info.values());
            int dpages = index_size(rowCount, index_col_info);

            System.out.println("dpages = " + dpages + ", cost = " + cost);

            String indexName = viewName + "_cidx";
            Q += "create unique clustered index " + indexName + " on " + viewName + " (";
            for (Iterator<String> iter1 = aliases.iterator(); iter1.hasNext();) {
                Q += iter1.next();
                if (iter1.hasNext()) Q += ",";
            }
            Q += " ) with statistics_only\n";

            indexMap.put(indexName, new int[]{rowCount, dpages, 1});

            //System.out.println("implement_configuration " + index_col_info + " " + dpages + " " + row_cnt);
            // indexMap.put(indexName, new int[]{rowCount, dpages});
            // System.out.println("autopilot: dpages " + dpages + " rowcnt " + row_cnt);
        }

        // select o_orderdate, ps_supplycost , COUNT_BIG(*) as count_big from dbo.partsupp, dbo.supplier, dbo.orders group by o_orderdate, ps_supplycost

        if (Q.length() > 0) {
            try {
                long time = System.currentTimeMillis();
                System.out.println("Q = " + Q);
                stmt.execute(Q);
                System.out.println("time = " + (System.currentTimeMillis() - time));
            } catch (SQLException e) {

                for (Iterator iterator = config.getMaterializedViews().iterator(); iterator.hasNext();) {
                    MatView view = (MatView) iterator.next();
                    view.setImplementedName(null);
                }
                System.err.println("SQL = " + Q);
                throw e;
            }
        }

        stmt.close();
    }

    private ZQuery removeInvalidSelects(ZQuery zQuery) {
        Vector selects = zQuery.getSelect();
        for (Iterator iterator = selects.iterator(); iterator.hasNext();) {
            ZSelectItem item = (ZSelectItem) iterator.next();
            if (item.getAggregate() != null) {
                if (item.getAggregate().equalsIgnoreCase("SUM")) {
                    List columns = ZqlUtils.getUsedColumns(item.getExpression(), false);
                    for (int i = 0; i < columns.size(); i++) {
                        String column = (String) columns.get(i);
                        String tableName = ColumnsGatherer.getTableName(column);
                        table_info ti = (table_info) globalInfo.global_table_info.get(tableName);
                        if (ti != null) {
                        	ColumnInfo ci = (ColumnInfo) ti.col_info.get(column.toUpperCase());
                            if (ci.nullable) {
                                iterator.remove();
                                break;
                            }
                        }
                    }
                } else {
                    iterator.remove();
                }
            }
        }

        zQuery.addSelect(selects);
        return zQuery;
    }

    public void disable_configuration(Configuration config, Connection conn) {
        int count = 0;
        //System.out.println("drop_configuration: configuration = " + configuration);
        try {
            String Q = new String();
            Set<String> indexNames = config.implementedIndexes.keySet();
            Statement stmt = conn.createStatement();
            for (String indexName : indexNames) {
                indexName = indexName.substring(indexName.indexOf('.') + 1);
                Q += "update sysindexes set status = 10485792 where name = '" + indexName + "'\n";
            }

            stmt.execute(Q);
            //System.out.println("\tdrop_configuration " + Q);
            stmt.close();
        }
        catch (Exception E) {
            System.out.println("drop_configuration: " + E.getMessage());
            E.printStackTrace();
        }
    }

    public void drop_configuration(PhysicalConfiguration configuration, Connection conn) {
        if (configuration.isEmpty()) return;
        //System.out.println("drop_configuration: configuration = " + configuration);
        try {
            String Q = new String();
            Statement stmt = conn.createStatement();
            for (Iterator<Index> it = configuration.indexes(); it.hasNext();) {
                Index idx = it.next();
                if (idx.isImplemented()) {
                    Q += "drop index " + idx.getTableName() + "." + idx.getImplementedName() + "\n";
                    idx.setImplementedName(null);
                }
            }

            for (Iterator iterator = configuration.getMaterializedViews().iterator(); iterator.hasNext();) {
                MatView view = (MatView) iterator.next();
                if (view.isImplemented()) {
                    Q += "drop view " + view.getImplementedName() + "\n";
                    view.setImplementedName(null);
                }
            }

            if (Q.length() != 0)
                stmt.execute(Q);
            //System.out.println("\tdrop_configuration " + Q);
            stmt.close();
        }
        catch (Exception E) {
            System.out.println("drop_configuration: " + E.getMessage());
            E.printStackTrace();
        }
    }

    //rewrite a query to use the indexes in a config
    public String prepareQueryForEnumeration(QueryDesc QD, PhysicalConfiguration config, boolean iac, boolean nlj) {
        Vector fromVector = QD.parsed_query.getFrom();
        ZFromItem[] from = (ZFromItem[]) fromVector.toArray(new ZFromItem[fromVector.size()]);
        Vector newFrom = new Vector();

        for (int i = 0; i < from.length; i++) {
            //String tableString = from[i].getTable().toUpperCase();
            String tableString = from[i].getTable();
            String alias = from[i].getAlias();

            if (alias != null)
                tableString += (" " + alias.toUpperCase());

            //System.out.println("RewriteQueryForEnum TS " + tableString + " " + config);

            if (config.getIndexesForTable(tableString) != null) {
                Index idx = config.getFirstIndexForTable(tableString);
                newFrom.add(new ZFromItem(tableString + " with (index (" + idx.getImplementedName() + "))"));
            } else {
                newFrom.add(new ZFromItem(tableString + " with (index (0))"));
            }
        }

        String result;
        QD.parsed_query.addFrom(newFrom);

        //System.out.println("prepareQueryForConfiguration: " + QD.parsed_query.getFrom());
        result = QD.parsed_query.toString();
        QD.parsed_query.addFrom(fromVector);

        if( !nlj )
            result = result + (iac ? "\noption (merge join, hash join)" : "\n option (merge join, hash join)");
        else
            result = result + (iac ? "\noption(force order)" : "");


        return result;
    }

    public synchronized String prepareQueryForConfiguration(QueryDesc QD, PhysicalConfiguration config, boolean iac, boolean nlj) {
        Vector fromVector = QD.parsed_query.getFrom();
        ZFromItem[] from = (ZFromItem[]) fromVector.toArray(new ZFromItem[fromVector.size()]);
        Vector newFrom = new Vector();

        for (int i = 0; i < from.length; i++) {
            String tableName = from[i].getTable().toLowerCase();
            String tableString = tableName;
            String alias = from[i].getAlias();
            if (alias != null)
                tableString += (" " + alias.toLowerCase());

            // System.out.println("rewriteq: ts " + tableString);

            Index index = config.getFirstIndexForTable(tableName);
            if (index != null && index.isImplemented()) {
                String indexName = index.getImplementedName();
                indexName = indexName.substring(indexName.indexOf('.') + 1);
                //System.out.println("rewriteq: index " + indexName);
                newFrom.add(new ZFromItem(tableString + " with (index (" + indexName + "))"));
            } else {
                newFrom.add(new ZFromItem(tableString + " with (index (0))"));
            }
        }


        QD.parsed_query.addFrom(newFrom);
        //System.out.println("prepareQueryForConfiguration: " + QD.parsed_query.getFrom());
        String result = new String(QD.parsed_query.toString());
        QD.parsed_query.addFrom(fromVector);

        if (QD.parsed_query.toString().contains("with")) {
            System.err.println("The query already contains a with!!!");
            Thread.dumpStack();
        }

        if(nlj) {

        }

        //System.out.println("rewriteq: " + result);
        return result;
    }

    public void init_database(Connection conn) {
        globalInfo = new MSGlobalInfo(conn);
    }

    public int index_size(table_info theBaseTableInfo, ArrayList theColInfo) {
        //Variables to calculate the non-clustered index size
        int Num_Rows = theBaseTableInfo.row_count;
        return index_size(Num_Rows, theColInfo);
    }

    public int getIndexSize(Index index) {
        return this.index_size(index, globalInfo);
    }

    public int index_size(int num_Rows, ArrayList theColInfo) {
        int Num_Key_Cols = theColInfo.size();
        int Fixed_Key_Size = 0;
        int Num_Variable_Key_Cols = 0;
        int Max_Var_Key_Size = 0;

        int Index_Null_Bitmap = 0;
        int Variable_Key_Size = 0;
        int NL_Index_Row_Size = 0;//Non-leaf index row size
        int NL_Index_Rows_Per_Page = 0;//non-leaf index rows per page
        int Index_Row_Size = 0;//leaf index row size
        int CIndex_Row_Size = 0;//row size for the clustered index of the table
        int Index_Rows_Per_Page = 0;//leaf level index rows per page
        int Num_Pages_Level_i = 0;//number of pages per row...

        //Variables relevant to the clustered index row size
        int Num_CKey_Cols = 0;
        int Fixed_CKey_Size = 0;
        int Num_Variable_CKey_Cols = 0;
        int Max_Var_CKey_Size = 0;
        int Variable_CKey_Size = 0;

        for (int i = 0; i < theColInfo.size(); i++) {
        	ColumnInfo c = (ColumnInfo) theColInfo.get(i);
            int size = c.col_size;
            if (size > 0) {
                Fixed_Key_Size += size;
            } else {
                Num_Variable_Key_Cols++;
                Max_Var_Key_Size += (-size);
            }
        }

        Index_Null_Bitmap = 2 + ((Num_Key_Cols + 7) / 8);

        if (Num_Variable_Key_Cols == 0)
            Variable_Key_Size = 0;
        else
            Variable_Key_Size = 2 + (Num_Variable_Key_Cols * 2) + Max_Var_Key_Size;

        NL_Index_Row_Size = Fixed_Key_Size + Variable_Key_Size + Index_Null_Bitmap + 1 + 8;
        NL_Index_Rows_Per_Page = (8096) / (NL_Index_Row_Size + 2);

        /*
          ArrayList theClustInfo = get_clust_info (table_name, theBaseTableInfo);
          Num_CKey_Cols = theClustInfo.size();
          for (int i=0; i<theClustInfo.size(); i++)
              {
              column_info c = (column_info) theClustInfo.get(i);
              int size = c.col_size;
              if (size > 0)
                  {
              Fixed_CKey_Size += size;
                  }
              else
                  {
              Num_Variable_CKey_Cols++;
              Max_Var_CKey_Size+=(-size);
                  }
              }
              int CIndex_Null_Bitmap = 2 + ((Num_CKey_Cols + 7)/8);
          if (Num_Variable_CKey_Cols > 0)
              Variable_CKey_Size = 2 + (Num_Variable_CKey_Cols * 2) + Max_Var_CKey_Size;
          else
              Variable_CKey_Size = 0;
             CIndex_Row_Size = Fixed_CKey_Size + Variable_CKey_Size + CIndex_Null_Bitmap + 1 + 8;
          */

        Index_Row_Size = CIndex_Row_Size + Fixed_Key_Size + Variable_Key_Size + Index_Null_Bitmap + 1;
        Index_Rows_Per_Page = (8096) / (Index_Row_Size + 2);
        Num_Pages_Level_i = num_Rows / Index_Rows_Per_Page;
        int pages = Num_Pages_Level_i;
        while ((Num_Pages_Level_i = Num_Pages_Level_i / NL_Index_Rows_Per_Page) >= 1)
            pages += Num_Pages_Level_i;

        return pages;
    }


    int index_size(Index idx, MSGlobalInfo global_info) {
        int count = 0;
        String Q = new String();
        Configuration result = new Configuration();
        ArrayList index_data = new ArrayList();
        int total_size = 0;

        //System.out.println("implement_configuration: config = " + configuration);

        try {
            int i = 0;
            table_info ti = (table_info) global_info.global_table_info.get(idx.getTableName());
            int row_cnt = ti.row_count;
            //for each ColumnSet in the given TableSet
            ArrayList index_col_info = new ArrayList();
            String[] attributes = (String[]) idx.getColumns().toArray(new String[idx.getColumns().size()]);
            for (int ai = 0; ai < attributes.length; ai++) {
                Object col_info = ti.col_info.get(attributes[ai]);
                index_col_info.add(col_info);
            }
            int dpages = index_size(ti, index_col_info);

            if (dpages == 0) {
                //System.out.println("implement_config " + index_col_info + " " + tableName);
                dpages = 1;//this happens only for 'region' or something with 10 records...
            }
            //System.out.println("autopilot: dpages " + dpages + " rowcnt " + row_cnt);

            total_size += dpages;
            count++;
            return total_size;
        }
        catch (Exception E) {
            System.out.println("config_size: " + E.getMessage());
            E.printStackTrace();
            return -1;
        }
    }

    public void removeQueryPrepareation(QueryDesc qd) {
    }

    public void prepareConnection(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(MS_CONNECTION_INIT);
        stmt.close();
    }
}