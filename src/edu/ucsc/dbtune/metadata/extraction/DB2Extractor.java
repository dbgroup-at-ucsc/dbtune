package edu.ucsc.dbtune.metadata.extraction;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.HashMap;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;

/**
 * Metadata extractor for DB2.
 * <p>
 * This class assumes that a DB2 system version 9.7 or greater is on the backend and connections
 * created using the DB2 JDBC driver (type 4) version 9.7.5 or greater.
 *
 * @author Ivo Jimenez
 */
public class DB2Extractor extends GenericJDBCExtractor
{
    /**
     * {@inheritDoc}
     */
    @Override
    protected void extractCatalog(Catalog catalog, Connection connection) throws SQLException
    {
        Statement stm = connection.createStatement();
        ResultSet rs = stm.executeQuery("SELECT current server FROM sysibm.sysdummy1");

        if (!rs.next())
            throw new SQLException("Impossible to determine catalog name for DB2 instance");

        catalog.setName(rs.getString(1));

        rs.close();
        stm.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void extractSchemas(Catalog catalog, Connection connection) throws SQLException
    {
        Map<String, Schema> schemaNamesToSchemas;

        DatabaseMetaData jdbcMetaData;
        ResultSet rs;
        Schema schema;
        String schemaName;

        jdbcMetaData = connection.getMetaData();

        if (jdbcMetaData == null) {
            throw new SQLException("Connection " + connection + " doesn't handle JDBC metadata");
        }

        schemaNamesToSchemas = new HashMap<String, Schema>();
        rs = jdbcMetaData.getSchemas();

        while (rs.next()) {
            schemaName = rs.getString("TABLE_SCHEM");

            if (schemaName.matches("DB2INST*") ||
                    schemaName.equals("NULLID") ||
                    schemaName.equals("SQLJ") ||
                    schemaName.equals("SYSCAT") ||
                    schemaName.equals("SYSFUN") ||
                    schemaName.equals("SYSIBM") ||
                    schemaName.equals("SYSIBMADM") ||
                    schemaName.equals("SYSIBMINTERNAL") ||
                    schemaName.equals("SYSIBMTS") ||
                    schemaName.equals("SYSPROC") ||
                    schemaName.equals("SYSPUBLIC") ||
                    schemaName.equals("SYSSTAT") ||
                    schemaName.equals("SYSTOOLS"))
                continue;

            schema = schemaNamesToSchemas.get(schemaName);

            if (schema == null) {
                schema = new Schema(catalog, schemaName);
                schemaNamesToSchemas.put(schemaName, schema);
            }
        }

        rs.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void extractObjectIDs(Catalog catalog, Connection connection) throws SQLException
    {
        // NOTE: DB2 assigns IDs only to tables.

        Statement stm;
        ResultSet rs;
        String    cmd;
        int       position = 0;
        int       counter = 0;

        catalog.setInternalID(1);

        for (Schema sch : catalog) {

            sch.setInternalID(counter++);

            for (Table tbl : sch.tables()) {

                stm = connection.createStatement();

                cmd = 
                    " SELECT" +
                    "    t.tableid" +
                    " FROM" +
                    "    syscat.tables t" +
                    " WHERE" +
                    "     t.tabname = '" + tbl.getName() + "'" +
                    " AND t.tabschema = '" + sch.getName() + "'";

                rs = stm.executeQuery(cmd);

                while (rs.next()) {
                    tbl.setInternalID(rs.getInt("tableid"));
                }

                stm.close();

                position = 0;
                for (Column col : tbl) {
                    col.setInternalID(position++);
                }
            }

            for (Index index : sch.indexes()) {
                index.setInternalID(counter++);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void extractBytes(Catalog catalog, Connection connection) throws SQLException
    {
        // throw new SQLException("Not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void extractPages(Catalog catalog, Connection connection) throws SQLException
    {
        // throw new SQLException("Not implemented yet");
    }
}
