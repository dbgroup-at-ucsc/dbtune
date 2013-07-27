package edu.ucsc.dbtune.workload;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * A reader of SQL statements executed against a DB2 database. The class relies on DB2's system 
 * snapshots facility (DB2 9.5 and up).
 *
 * @author Ivo Jimenez
 */
public class DB2WorkloadReader extends QueryLogReader
{
    private Connection connection;

    /**
     * In order to create an object, an already-opened connection to a DB2 system must be provided. 
     * The user associated to the connection must have the right privileges in order to obtain 
     * system snapshots from the underlying DB2 system. Consult the DB2 manual for more about the 
     * authorization requirements.
     * <p>
     * If any of the above assumptions doesn't hold, the {@link #hasNewStatement} method will raise 
     * a {@link SQLException}.
     *
     * @param connection
     *      an already-opened connection to a DB2 database
     * @throws SQLException
     *      if {@link Connection#getMetaData} throws an exception
     */
    public DB2WorkloadReader(Connection connection) throws SQLException
    {
        super(new Workload(connection.getMetaData().getURL() + "_" + (new Date()).toString()));

        this.sqls = new ArrayList<SQLStatement>();
        this.connection = connection;

        startWatcher(10);
    }

    /**
     * {@inheritDoc}
     */
    protected List<SQLStatement> hasNewStatement() throws SQLException
    {
        List<SQLStatement> newStmts = new ArrayList<SQLStatement>();
        String sql;

        sql =
            "SELECT " +
            "   stmt_text " +
            " FROM " +
            "   sysibmadm.snapdyn_sql";

        Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery(sql);
        int counter = 1;

        while (rs.next()) {
            String stmt = rs.getString("stmt_text").toLowerCase().trim();

            if (stmt.contains("sysibmadm.") ||
                    stmt.contains("systools.") ||
                    stmt.contains("syscat.") ||
                    stmt.startsWith("flush") ||
                    stmt.startsWith("call") ||
                    stmt.startsWith("set"))
                continue;

            newStmts.add(
                    new SQLStatement(rs.getString("stmt_text"), workload, sqls.size() + counter));
            counter++;
        }

        rs.close();

        s.execute("FLUSH PACKAGE CACHE DYNAMIC");
        s.close();

        return newStmts;
    }
}
