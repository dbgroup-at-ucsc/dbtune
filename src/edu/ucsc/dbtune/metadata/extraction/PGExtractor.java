/* ************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.metadata.extraction;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.HashMap;

/**
 * Metadata extractor for PostgreSQL.
 * <p>
 * This class assumes a PostgreSQL system version 8.3 or greater is on the backend and connections
 * created using the postgres' JDBC driver (type 4) version 9.0 or greater.
 * <p>
 * The PostgreSQL's JDBC driver doesn't implement the {@link java.sql.DatabaseMetaData#tableIndexStatistic} type for the 
 * {@link java.sql.DatabaseMetaData#getIndexInfo} method. This class extracts metadata information about indexes and 
 * table/column statistics.
 * <p>
 * The way that metadata is extracted is by querying the PostgreSQL's system tables, in particular, tables {@code pg_index}, 
 * <code>pg_class</code> and <code>pg_attribute</code>. An exception will be thrown if these tables don't exist or if the 
 * column names for them change. In general if the schema for PostgreSQL's system tables change, it is very likely that the 
 * methods of this class will fail.
 *
 * @see GenericJDBCExtractor
 *
 * @author Ivo Jimenez
 */
public class PGExtractor extends GenericJDBCExtractor
{
    /**
     * {@inheritDoc}
     */
    @Override
    protected void extractCatalog(Catalog catalog, Connection connection) throws SQLException
    {
        // nothing to add
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void extractSchemas(Catalog catalog, Connection connection) throws SQLException
    {
        Map<String,Schema>  schemaNamesToSchemas;

        DatabaseMetaData jdbcMetaData;
        ResultSet        rs;
        Schema           schema;
        String           schemaName;

        jdbcMetaData = connection.getMetaData();

        if (jdbcMetaData == null)
        {
            throw new SQLException("Connection " + connection + " doesn't handle JDBC metadata");
        }

        schemaNamesToSchemas = new HashMap<String,Schema>();
        rs                = jdbcMetaData.getSchemas();

        while (rs.next())
        {
            schemaName = rs.getString("TABLE_SCHEM");

            if(schemaName == null) {
                schemaName = "default";
            }

            schema = schemaNamesToSchemas.get(schemaName);

            if(schema == null) {
                schema = new Schema(schemaName);
                schemaNamesToSchemas.put(schemaName,schema);
                catalog.add(schema);
            }
        }

        rs.close();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void extractBytes(Catalog catalog, Connection connection) throws SQLException
    {
        // nothing yet
    }

    /**
     */
    @Override
    public void extractPages(Catalog catalog, Connection connection)
        throws SQLException
    {
        Index     index;
        Statement stm;
        ResultSet rs;
        String    cmd;
        String    name;

        for(Schema schema : catalog.getSchemas()) {
            for(Table table : schema.getTables()) {

                stm = connection.createStatement();
                cmd = 
                    " SELECT" +
                    "    t.relname," +
                    "    t.relpages," +
                    "    t.reltuples" +
                    " FROM" +
                    "    pg_class t," +
                    "    pg_namespace s" +
                    " WHERE" +
                    "     t.relname = '" + table.getName() + "'" +
                    " AND s.nspname = '" + schema.getName() + "'" +
                    " AND t.relnamespace = s.oid";

                rs = stm.executeQuery(cmd);

                while (rs.next())
                {
                    table.setCardinality(rs.getInt("reltuples"));
                    table.setPages(rs.getInt("relpages"));
                }

                stm.close();

                stm = connection.createStatement();
                cmd = 
                    " SELECT" +
                    "   t.relname as tname," +
                    "   i.relname as iname," +
                    "   i.reltuples," +
                    "   i.relpages," +
                    "   i.relname" +
                    " FROM" +
                    "    pg_class     t," +
                    "    pg_class     i," +
                    "    pg_index     ix," +
                    "    pg_namespace s" +
                    " WHERE" +
                    "      t.oid      = ix.indrelid" +
                    "  AND i.oid      = ix.indexrelid" +
                    "  AND t.relkind  = 'r'" +
                    "  AND t.relname  = '" + table.getName() + "'" +
                    "  AND s.nspname = '" + schema.getName() + "'" +
                    "  AND t.relnamespace = s.oid" +
                    " ORDER BY" +
                    "    t.relname," +
                    "    i.relname";

                rs = stm.executeQuery(cmd);
                index = null;

                while (rs.next())
                {
                    name  = rs.getString("iname");
                    index = table.findIndex(name);

                    if (index == null)
                    {
                        throw new SQLException("Index " + name + " not in " + table);
                    }

                    index.setPages(rs.getInt("relpages"));
                    index.setCardinality(rs.getInt("reltuples"));
                }

                stm.close();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void extractCardinality(Catalog catalog, Connection connection) throws SQLException
    {
        super.extractColumnCardinality(catalog,connection);
        // table and index cardinality already in #extractPages
    }
}
