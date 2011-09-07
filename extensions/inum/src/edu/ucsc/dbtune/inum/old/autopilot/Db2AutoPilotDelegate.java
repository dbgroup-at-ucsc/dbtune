package edu.ucsc.dbtune.inum.old.autopilot;

import com.google.common.base.Joiner;
import com.ibm.db2.jcc.DB2ConnectionlessBlob;
import edu.ucsc.dbtune.inum.old.commons.Utils;
import edu.ucsc.dbtune.inum.old.model.Configuration;
import edu.ucsc.dbtune.inum.old.model.Index;
import edu.ucsc.dbtune.inum.old.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.old.model.Plan;
import edu.ucsc.dbtune.inum.old.model.QueryDesc;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.logging.Logger;
import org.apache.commons.dbutils.DbUtils;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Dec 10, 2007
 * Time: 7:13:30 PM
 * To change this template use File | Settings | File Templates.
 * <p/>
 * Message from GODs (Danny and Guy)...
 * I think what may be happening is that his filling in of ADVISE_INDEX is the problem such that we are ignoring the index due to not enough info, or he has a duplicate of an exiting index.
 * We are getting the following columns in the engine on EVALUATE INDEXES explain mode:
 * USE_INDEX
 * TBNAME
 * COLNAMES
 * NAME
 * UNIQUERULE
 * COLCOUNT
 * IID
 * REV_SCANS
 * UNIQUE_CLCNT
 * INDEXTYPE
 * INDEXEXISTS
 * RIDTOBLOCK
 * TBCREATOR
 * FIRSTKEYCARD
 * FULLKEYCARD
 * FIRST2KCARD
 * FIRST3KCARD
 * FIRST4KCARD
 * <p/>
 * He should have COLNAMES set and if he is making an index on columns A in ascending order and B in descending then the colnames should be '+A-B'
 * He should also have TBNAME, NAME,  TBCREATOR, NAME, and INDEXTYPE defined. We use these fields to determine if the virtual index may be the same as another virtual index or existing index. We will use the index if its not found to be the same as something already used in the server. The IID should be a unique number. He should set INDEXTYPE to 'REG' for a regular index. He can look at SYSCAT.INDEXES to get an idea what these fields mean. UNIQUERULE = 'D' for duplicates allowed or 'U' for unique index. For a unique index the UNIQUE_COLCOUNT should be equal to the number of columns in the key that form the unique key where the rest of the columns would be considered include columns. Otherwise for a non-unique index, we should have the number of index columns (COLCOUNT) = UNIQUE_COLCOUNT. REV_SCANS should be 'Y' or 'N' to denote yes or no to whether the index allows reverse scans. INDEXEXISTS should be set to 'N' for a virtual index and USE_INDEX needs to be set to 'Y'. Set RIDTOBLOCK = 'N' since I assume he is just working on rid indexes. As for the *CARD columns, if he sets them all to -1 then that will tell the explain mode to generate the statistics internally otherwise, we will use the statistics input in the ADVISE_INDEX table. If the index is used in the plan under evaluate indexes, a new ADVISE_INDEX entry is added for the same index where the stats for that index will be included in the ADVISE_INDEX entry.
 */
public class Db2AutoPilotDelegate extends AutoPilotDelegate {

    public static final String INSERT_VIRTUAL =
            "insert into advise_index" +
                    "(source_schema, tbname, tbcreator, name, colnames, use_index, uniquerule, COLCOUNT, unique_colcount, IID, REVERSE_SCANS, INDEXTYPE, EXISTS, RIDTOBLOCK, FIRSTKEYCARD, FULLKEYCARD, FIRST2KEYCARD, FIRST3KEYCARD, FIRST4KEYCARD, NLEAF)" +
                    "values('DB2ADMIN',    ?,      ?,         ?,    ?,        'Y',       ?,          ?,        ?,               ?,   'Y',          'REG',      'N',    'N'       , -1          , -1         , -1         , -1         ,     -1,            -1)";

    public static final String EXPLAIN_SQL = "SELECT O.operator_id, S2.target_id as parent, O.operator_type, S.object_name,\n" +
            "S.STREAM_COUNT as card,\n" +
            "O.total_cost as cost\n" +
            "FROM EXPLAIN_OPERATOR O\n" +
            "LEFT OUTER JOIN EXPLAIN_STREAM S2\n" +
            "ON O.operator_id = S2.source_id\n" +
            "LEFT OUTER JOIN EXPLAIN_STREAM S\n" +
            "ON O.operator_id = S.target_id\n" +
            "AND O.explain_time = S.explain_time\n" +
            "AND S.object_name IS NOT NULL\n" +
            "ORDER BY O.explain_time ASC, operator_id ASC";


    public static final String DROP_VIRTUAL =
            "delete from advise_index where colnames like ? and tbname = ?";

    public static final String DISABLE_VIRTUAL =
            " update table advise_index where name in ";

    public static final String STMT_PROFILE =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<OPTPROFILE VERSION=\"9.1.0.0\">\n" +
                    "  <STMTPROFILE ID=\"%s\">\n" +
                    "      <STMTKEY SCHEMA=\"%s\">\n" +
                    "          <![CDATA[%s]]>\n" +
                    "      </STMTKEY>\n" +
                    "      <OPTGUIDELINES>\n" +
                    "          %s\n" +
                    "      </OPTGUIDELINES>\n" +
                    "  </STMTPROFILE>\n" +
                    "</OPTPROFILE>";

    private static final Logger _log = Logger.getLogger("Db2AutoPilotDelegate");
    public static final String INSERT_PROFILE = " insert into systools.opt_profile(schema, name, profile) values (?,?,?)";
    private int count;
    private autopilot ap;
    private static final String SCHEMA_NAME = "DB2ADMIN";
    private int queryNo;
    private ThreadLocalOptProfile _profile = new ThreadLocalOptProfile();
    private Map<String, Integer> indexSizeCache = new HashMap();
    private DB2GlobalInfo globalInfo;
    private int _profileID = 0;

    public Db2AutoPilotDelegate(autopilot ap) {
        this.ap = ap;
    }

    public void setIndexSizeCache(Map map) {
        this.indexSizeCache = map;
    }

    public Map getIndexSizeCache() {
        return indexSizeCache;
    }

    public Plan getExecutionPlan(Connection conn, String Q) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("delete from explain_instance");
        Timestamp ts = getDatabaseTime(stmt);
        ResultSet rs;

        stmt.execute("FLUSH OPTIMIZATION PROFILE CACHE");
//        stmt.execute("Set current query optimization = 0");
        stmt.execute("set current explain mode = EVALUATE INDEXES");
        if (getActiveProfileName() != null) {
            stmt.execute("set current optimization profile = " + SCHEMA_NAME + "." + getActiveProfileName());
        }

        try {
            stmt.execute(Q);
        } catch (Throwable e) {
            // e.printStackTrace();
        }

        stmt.execute("set current explain mode=NO");
        rs = stmt.executeQuery(EXPLAIN_SQL);
        DB2Plan plan = new DB2Plan();
        while (rs.next()) {
            int rowId = rs.getInt(1);
            int parent = rs.getInt(2);
            String operator = rs.getString(3);
            String target = rs.getString(4);
            double card = rs.getDouble(5);
            float cost = rs.getFloat(6);

            plan.add(new DB2PlanDesc(rowId, parent, operator, target, card, cost));
        }

        //stmt.execute("delete from advise_index where create_time >= '" + ts.toString() + "'"); // desparation!

        stmt.close();

        plan.analyzePlan();
        return plan;
    }

    private Timestamp getDatabaseTime(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("select current timestamp from sysibm.sysdummy1");
        rs.next();
        Timestamp ts = rs.getTimestamp(1);
        rs.close();
        return ts;
    }

    public void implement_configuration(autopilot ap, PhysicalConfiguration configuration, Connection conn) {
        try {
            Statement stmt = conn.createStatement();
            Timestamp ts = getDatabaseTime(stmt);
            Set set = new HashSet();

            PreparedStatement pstmt = conn.prepareStatement(INSERT_VIRTUAL);

            int i = 0;
            for (String tableName : configuration.getIndexedTableNames()) {
                for (Index idx : configuration.getIndexesForTable(tableName)) {
                    int count;
                    if (idx.isImplemented()) continue;
                    synchronized (this) {
                        count = this.count++;
                    }
                    /*
                    String colNames = Join.join(", ", idx.getColumns());

                    final String query = "select " + colNames + " from " + tableName + " order by " + colNames;
                    stmt.execute("set current explain mode = RECOMMEND INDEXES");
                    try {
                        stmt.execute(query);
                    } catch (BugCheckException e) {
                        // ignore.
                    }
                    stmt.execute("set current explain mode = NO");
                    ResultSet rs = stmt.executeQuery("select name from advise_index where create_time > '" + ts.toString() +"' and tbname = '" + tableName.toUpperCase() +"' order by create_time queryString");
                    if (rs.next()) {
                        final String name = rs.getString(1);
                        if(!set.contains(name)) {
                            idx.setImplementedName(name);
                            set.add(idx.getImplementedName());
                        }
                    }
                    */
                    int uniqColCount = getUniqueColCount(idx);
                    String columnNames = "+" + Joiner.on("+").join(idx.getColumns());
                    String tabName = idx.getTableName().toUpperCase();
                    String tbcreator = "DB2ADMIN";
                    String name = "IDX" + this.count;
                    String uniqrule = idx.getColumns().size() == uniqColCount ? "U" : "D";
                    int COLCOUNT = idx.getColumns().size();
                    int IID = this.count;

                    pstmt.setString(1, tabName);
                    pstmt.setString(2, tbcreator);
                    pstmt.setString(3, name);
                    pstmt.setString(4, columnNames);
                    pstmt.setString(5, uniqrule);
                    pstmt.setInt(6, COLCOUNT);
                    pstmt.setInt(7, uniqColCount);
                    pstmt.setInt(8, IID);

                    pstmt.execute();

                    idx.setImplementedName(name);
                }

                i++;
            }
            stmt.close();
            pstmt.close();
        }
        catch (Exception E) {
            System.out.println("implement_configuration: " + E.getMessage());
            E.printStackTrace();
        }
    }

    private int getUniqueColCount(Index idx) {
        table_info ti = (table_info) globalInfo.global_table_info.get(idx.getTableName());

        boolean iskey = true;
        for (Iterator iterator = ti.pkeys.iterator(); iterator.hasNext();) {
        	ColumnInfo ci = (ColumnInfo) iterator.next();
            if (!idx.getColumns().contains(ci.getColName())){
                iskey = false;
                break;
            }
        }

        return iskey ? ti.pkeys.size() : idx.getColumns().size();
    }

    public void disable_configuration(Configuration config, Connection conn) {

    }

    public void drop_configuration(PhysicalConfiguration config, Connection conn) {
        try {
            //PreparedStatement stmt = conn.prepareStatement(DROP_VIRTUAL);
            Statement stmt = conn.createStatement();
            for (String tableName : config.getIndexedTableNames()) {
                for (Index idx : config.getIndexesForTable(tableName)) {
                    if (idx.isImplemented()) {
                        stmt.addBatch("delete from advise_index where name = '" + idx.getImplementedName() + "' and tbname = '" + idx.getTableName().toUpperCase() + "'");
                        idx.setImplementedName(null);
                    }
                }
            }
            stmt.executeBatch();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //rewrite a query to use the indexes in a config
    public String prepareQueryForEnumeration(QueryDesc QD, PhysicalConfiguration config, boolean iac, boolean nlj) {
        return this.prepareQueryForConfiguration(QD, config, iac, nlj);
    }

    public String prepareQueryForConfiguration(QueryDesc QD, PhysicalConfiguration config, boolean iac, boolean nlj) {
        StringBuffer indexBuffer = new StringBuffer();
        Set usedTables = QD.used.getIndexedTableNames();

        if(!iac) {
            for (Iterator iterator = usedTables.iterator(); iterator.hasNext();) {
                String table = (String) iterator.next();

                if(config != null) {
                    Index idx = config.getFirstIndexForTable(table);
                    if (idx != null && idx.isImplemented()) {
                        indexBuffer.append("<IXSCAN TABLE=\"" + table + "\" INDEX=\"" + idx.getImplementedName() + "\"/>"+ Utils.NL);
                    }
                } else {
                    indexBuffer.append("<TBSCAN TABLE=\""+table+"\">"+ Utils.NL);
                }
            }            
        } else {
            // make sure the index is accessed...
            Stack<String> stack = new Stack();

            Index idx = config.indexes().next();
            // first add the indexed table.
            stack.push("<IXSCAN TABLE=\"" + idx.getTableName() + "\" INDEX=\"" + idx.getImplementedName() + "\"/>");

            for (Iterator iterator = usedTables.iterator(); iterator.hasNext();) {
                String table = (String) iterator.next();
                if(!idx.getTableName().equals(table)) {
                    stack.push("<TBSCAN TABLE=\"" + table + "\"/>");
                }
                if(stack.size() == 2) {
                    String table1 = stack.pop();
                    String table2 = stack.pop();
                    StringBuffer buf = new StringBuffer();
                    String joinMethod = nlj ? "NLJOIN" : "HSJOIN";
                    buf.append("<").append(joinMethod).append(">");
                    buf.append(table1).append(table2);
                    buf.append("</").append(joinMethod).append(">");
                    stack.push(buf.toString());
                }
            }
            indexBuffer.append(stack.pop());
        }

        String profName = getProfileName();
        String queryStr = QD.parsed_query.toString();
        String profile = String.format(STMT_PROFILE, profName, SCHEMA_NAME, queryStr, indexBuffer.toString());
        Connection conn = ap.getConnection();
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("delete from systools.opt_profile where name='" + profName + "'");
            stmt.close();

            Blob clob = new DB2ConnectionlessBlob(profile.getBytes());

            PreparedStatement pstmt = conn.prepareStatement(INSERT_PROFILE);
            pstmt.setString(1, SCHEMA_NAME);
            pstmt.setString(2, profName);
            pstmt.setBlob(3, clob);

            pstmt.execute();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (Exception e) {
            }
        }

        setActiveProfileName(profName);
        _log.info("prepareQueryForConfiguration: " + profile);
        // throw new UnsupportedOperationException();
        return queryStr;
    }

    public void removeQueryPrepareation(QueryDesc qd) {
        Connection conn = ap.getConnection();
        String sql = "delete from systools.opt_profile where schema = '" + SCHEMA_NAME + "' and name='";
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql + getProfileName() + "'");
            stmt.close();
        } catch (SQLException e) {
            System.err.println("SQL: " + sql);
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
            }
        }
        setActiveProfileName(null);
    }

    public synchronized int getQueryNo() {
        return queryNo++;
    }

    public synchronized String getProfileName() {
        return ("PROFILE" + (_profileID++)).toUpperCase();
    }

    public String getActiveProfileName() {
        return _profile.get();
    }

    public void setActiveProfileName(String str) {
        _profile.set(str);
    }

    private class ThreadLocalOptProfile extends ThreadLocal<String> {
        public String initialValue() {
            return getProfileName();
        }
    }

    @Override
    public int getIndexSize(Index index) {
        if (indexSizeCache.containsKey(index.getKey())) {
            return indexSizeCache.get(index.getKey());
        }
        int size = Integer.MAX_VALUE;
        Connection conn = ap.getConnection();
        boolean locallyImplemented = false;
        try {
            PhysicalConfiguration config = null;
            if (!index.isImplemented()) {
                config = new PhysicalConfiguration();
                config.addIndex(index);
                this.implement_configuration(ap, config, conn);
                locallyImplemented = true;
            }

            // sometimes we still don't get an index implemented.
            if (index.isImplemented()) {
                table_info ti = (table_info) globalInfo.global_table_info.get(index.getTableName());
                int keySize = 0;
                for (Iterator iterator = index.getColumns().iterator(); iterator.hasNext();) {
                    String column = (String) iterator.next();
                    ColumnInfo ci = (ColumnInfo) ti.col_info.get(column);
                    if (ci.colType.equals("VARCHAR")) {
                        keySize += ci.col_size + 2;
                    } else {
                        keySize += ci.col_size;
                    }
                }

                // evaluate a query which is definitely going to use this index.
                String columns = Joiner.on(",").join(index.getColumns());
                String query = "select " + columns + " from " + index.getTableName() + " order by " + columns;
                // get the best statistics from the system.
                Plan plan = this.getExecutionPlan(conn, query);
                if (plan.getAccessCost(index.getImplementedName()) <= 0.0) {
                    // we have a new index in the advise index table, with the nleaf set.
                    // we have to guess the number of leaves in the table.
                    size = estimate_leaf_size(keySize, ti.row_count);
                } else {
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("select NLEAF  from  advise_index where name = '" + index.getImplementedName() + "' and nleaf > 0");
                    if (rs.next()) {
                        size = rs.getInt(1);
                    }
                    rs.close();
                    stmt.close();
                }

                size += estimate_nonleaf_size(size, keySize, getUniqueColCount(index) == index.getColumns().size());
            }

            if(locallyImplemented) {
                ap.drop_configuration(config);
            }
            _log.info("getIndexSize: " + index + ", " + size);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                DbUtils.close(conn);
            } catch (Exception ex) {
            }
        }

        indexSizeCache.put(index.getKey(), size);
        return size;
    }

    private int estimate_leaf_size(int keySize, int row_count) {
        double spacePerKey = keySize + RID_LENGTH + 3;
        double usableSpacePerPage = Math.floor((100 - PCTFREE) * 4038 / 100);
        double keysPerPage = Math.floor(usableSpacePerPage / spacePerKey);
        return (int) Math.ceil(row_count / keysPerPage);
    }

    public static final float PCTFREE = 0.0f;
    public static final float RID_LENGTH = 4;

    public int estimate_nonleaf_size(int nleaf, int k, boolean unique) {
        // k = avgkeylength
        // n = avg number of keys per unique entry.
        double spacePerKey = k + (unique ? 0 : RID_LENGTH) + 7;
        double useableSpace = Math.floor(Math.max(10, (100 - PCTFREE) * 4046 / 100));
        double entriesPerPage = Math.floor(useableSpace / spacePerKey);
        double minChildPages = Math.max(2, (entriesPerPage + 1));
        int currentChildPages = nleaf;
        int totalPages = nleaf;
        while (currentChildPages > 1) {
            currentChildPages = (int) Math.ceil(currentChildPages / minChildPages);
            totalPages += currentChildPages;
        }

        return totalPages + 2;
    }

    @Override
    public void init_database(Connection conn) {
        globalInfo = new DB2GlobalInfo(conn);
    }
}
