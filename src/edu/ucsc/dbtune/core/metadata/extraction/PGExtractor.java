/*
 * ****************************************************************************
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
 * ****************************************************************************
 */

package edu.ucsc.dbtune.core.metadata.extraction;

import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.metadata.Catalog;
import edu.ucsc.dbtune.core.metadata.Table;
import edu.ucsc.dbtune.core.metadata.Column;
import edu.ucsc.dbtune.core.metadata.Index;

import java.util.List;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Metadata extractor for PostgreSQL.
 * <p>
 * This class assumes a PostgreSQL system version 8.3 or greater is on the backend and JDBC driver 
 * (type 4) version.
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class PGExtractor extends GenericJDBCExtractor
{
    /**
     * The PostgreSQL's JDBC driver doesn't implement the {@link 
     * java.sql.DatabaseMetaData#tableIndexStatistic} type for the {@link 
     * java.sql.DatabaseMetaData#getIndexInfo} method. This method extracts metadata information 
     * about indexes and table/column statistics.
     * <p>
     * The way that metadata is extracted is through the querying of PostgreSQL's system tables, in 
     * particular, tables <code>pg_index</code>, <code>pg_class</code> and 
     * <code>pg_attribute</code>. An exception will be thrown if these tables don't exist or if the 
     * column names for them change. In general if the schema for PostgreSQL's system tables change, 
     * it is very likely that this method will fail.
     *
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     if a connection error occurs when reading PostgreSQL's system tables.
     * @see GenericJDBCExtractor
     */
    @Override
    public Catalog extract( DatabaseConnection connection )
        throws SQLException
    {
        List<Table>         tables;
        Catalog             catalog;
        Index               index;
        java.sql.Connection con;
        Statement           stmnt;
        ResultSet           rsset;
        String              cmmnd;
        String              name;

        try
        {
            catalog = super.extract(connection);
            con     = connection.getJdbcConnection();
            tables  = catalog.getSchemas().get(0).getTables();

            for (Table table : tables )
            {
                stmnt = con.createStatement();
                cmmnd = 
                    " SELECT" +
                    "    t.relname," +
                    "    t.relpages," +
                    "    t.reltuples" +
                    " FROM" +
                    "    pg_class t" +
                    " WHERE" +
                    "    t.relname  = '" + table.getName() + "'";

                rsset = stmnt.executeQuery(cmmnd);
                index = null;

                while (rsset.next())
                {
                    table.setCardinality(rsset.getInt("reltuples"));
                    table.setPages(rsset.getInt("relpages"));
                }

                stmnt.close();

                stmnt = con.createStatement();
                cmmnd = 
                    " SELECT" +
                    "   t.relname as tname," +
                    "   i.relname as iname," +
                    "   i.reltuples," +
                    "   i.relpages," +
                    "   i.relname" +
                    " FROM" +
                    "    pg_class     t," +
                    "    pg_class     i," +
                    "    pg_index     ix" +
                    " WHERE" +
                    "    t.oid      = ix.indrelid    AND" +
                    "    i.oid      = ix.indexrelid  AND" +
                    "    t.relkind  = 'r'            AND" +
                    "    t.relname  = '" + table.getName() + "'" +
                    " ORDER BY" +
                    "    t.relname," +
                    "    i.relname";

                rsset = stmnt.executeQuery(cmmnd);
                index = null;

                while (rsset.next())
                {
                    name  = rsset.getString("iname");
                    index = table.findIndex(name);

                    if (index == null)
                    {
                        throw new SQLException("Index " + name + " not in " + table);
                    }

                    index.setPages(rsset.getInt("relpages"));
                    index.setCardinality(rsset.getInt("reltuples"));
                }

                stmnt.close();

                for( Column column : table.getColumns() )
                {
                    stmnt = con.createStatement();
                    cmmnd =
                        " SELECT " +
                        "   count(DISTINCT " + column.getName() + ") " +
                        " FROM " +
                            table.getName();

                    rsset = stmnt.executeQuery(cmmnd);

                    while (rsset.next())
                    {
                        column.setCardinality(rsset.getLong(1));
                    }

                    stmnt.close();
                }
            }

            return catalog;
        }
        catch (Exception e)
        {
            throw new SQLException(e);
        }
    }
}
