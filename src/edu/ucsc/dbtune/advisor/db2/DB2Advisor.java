package edu.ucsc.dbtune.advisor.db2;


import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import java.util.Map;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.Advisor;
import edu.ucsc.dbtune.metadata.ByContentIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

/**
 * Generates a recommendation according to the db2advis program.
 * 
 * @author Ivo Jimenez
 */
public class DB2Advisor extends Advisor
{
    private DatabaseSystem dbms;

    /**
     * constructor.
     * 
     * @param dbms
     *      system connected representing a DB2 instance
     * @throws SQLException
     *      if the underlaying DBMS is not a DB2 instance
     */
    public DB2Advisor(DatabaseSystem dbms)
        throws SQLException
    {
        if (!(dbms.getOptimizer() instanceof DB2Optimizer) &&
                !(dbms.getOptimizer().getDelegate() instanceof DB2Optimizer))
            throw new SQLException("Expecting DB2Optimizer");

        this.dbms = dbms;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(Workload workload) throws SQLException
    {
        Statement stmt = dbms.getConnection().createStatement();
        
        stmt.execute("DELETE FROM systools.advise_index");
        stmt.execute("DELETE FROM systools.advise_workload");

        int i = 0;

        for (SQLStatement sql : workload) 
            stmt.execute(
                    "INSERT INTO systools.advise_workload VALUES(" +
                    "   'dbtuneworkload'," +
                    "    " + i++ + ", " +
                    "   '" + sql.getSQL().replace("'", "''") + "'," +
                    "   '',1,0,0,0,0,'')");
        
        stmt.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(SQLStatement sql) throws SQLException
    {   
        List<SQLStatement> sqls = new ArrayList<SQLStatement>();
        sqls.add(sql);
        Workload wl = new Workload(sqls);
        process(wl);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> getRecommendation(int budget) throws SQLException
    {   
        ResultSet rs;
        double sizeInMB;
        Map<String, Long> indexBytes;
        
        CallableStatement cstmt =
            dbms.getConnection().prepareCall(
                "CALL SYSPROC.DESIGN_ADVISOR(" +
                "   ?, ?, ?, blob(' " +
                "      <?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "      <plist version=\"1.0\"> " +
                "         <dict> " +
                "            <key>CMD_OPTIONS</key>" +
                "            <string>" +
                "               -workload  dbtuneworkload " +
                "               -disklimit " + budget +
                "               -type      I " +
                "               -compress  OFF " +
                //"               -qualifier tpcds " +
                "               -drop" +
                "            </string>" +
                "         </dict>" +
                "      </plist>'), " +
                "   NULL, ?, ?)");

        cstmt.setInt(1, 1);
        cstmt.setInt(2, 0);
        cstmt.setString(3, "en_US");
        cstmt.registerOutParameter(4, Types.BLOB);
        cstmt.registerOutParameter(5, Types.BLOB);
        
        rs = cstmt.executeQuery();
        indexBytes = new HashMap<String, Long>();
        
        while (rs.next()) {
            sizeInMB = Double.valueOf(rs.getString("DISKUSE").trim());
            indexBytes.put(rs.getString("NAME"), 
                        (long) (sizeInMB * Math.pow(2, 20)));
        }
        
        Set<ByContentIndex> unique = new HashSet<ByContentIndex>();
        
        DB2Optimizer optimizer;
        if (dbms.getOptimizer() instanceof DB2Optimizer)
            optimizer = (DB2Optimizer) dbms.getOptimizer();
        else 
            optimizer = (DB2Optimizer)dbms.getOptimizer().getDelegate();
        
        // TODO: add a knob when we do not want
        // to compute the index creation cost
        // avoid what-if optimization
        for (Index i : DB2Optimizer.readAdviseIndexTable(dbms.getConnection(), dbms.getCatalog(), indexBytes)){
            i.setCreationCost(DB2Optimizer.getCreationCost(optimizer, new HashSet<Index>(), i));
            unique.add(new ByContentIndex(i));
        }
        
        double space = 0;
        for (Index i : unique) 
            space += i.getBytes();
        
        System.out.println("Count:  " + unique.size());
        System.out.println("Budget: " + budget);
        System.out.println("Actual: " + space / Math.pow(2, 20));

        return new HashSet<Index>(unique);
    }
}
