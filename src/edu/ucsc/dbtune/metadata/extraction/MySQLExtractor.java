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
import edu.ucsc.dbtune.metadata.Column;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Metadata extractor for MySQL.
 * <p>
 * This class assumes a MySQL system version 5.5 or greater is on the backend and that JDBC 
 * connections are using the MySQL's JDBC driver (type 4) version 5.1 or greater.
 * <p>
 * The user associated to the {@link Connection} object needs 'read' privileges on the {@code 
 * information_schema} database (schema in DBTune's terminology; database in MySQL lingo).
 *
 * @author Ivo Jimenez
 */
public class MySQLExtractor extends GenericJDBCExtractor
{
    int idCounter=1;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void extractCatalog(Catalog catalog, Connection connection) throws SQLException
    {
        catalog.setName("mysql");
        catalog.setInternalID(idCounter++);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void extractSchemas(Catalog catalog, Connection connection) throws SQLException
    {
        DatabaseMetaData meta;
        ResultSet        rs;
        String           schName;

        meta = connection.getMetaData();

        if (meta == null)
        {
            throw new SQLException("Connection " + connection + " doesn't handle JDBC metadata");
        }

        rs = meta.getCatalogs();

        while (rs.next())
        {
            schName = rs.getString("table_cat");

            if(schName.equals("information_schema") || schName.equals("mysql")) {
                continue;
            }

            Schema sch = new Schema(catalog,schName);

            sch.setInternalID(idCounter++);
        }

        rs.close();

        swappedTerms = true;
    }

    /**
     * {inheritDoc}
     */
    @Override
    protected void extractIndexes(Catalog catalog, Connection connection) throws SQLException
    {
        super.extractIndexes(catalog,connection);

        for(Schema sch : catalog.getSchemas()) {
            for(Table tbl : sch.getTables()) {
                tbl.setInternalID(idCounter++);
                for(Index idx : tbl.getIndexes()) {
                    if(idx.getName().equals("PRIMARY")) {
                        idx.setName(tbl.getName() + "_pkey");
                        idx.setInternalID(idCounter++);
                    }
                }
                for(Column col : tbl.getColumns()) {
                    col.setInternalID(idCounter++);
                }
            }
        }
    }

    @Override
    protected void extractObjectIDs(Catalog catalog, Connection connection) throws SQLException
    {
        // nothing yet
    }

    @Override
    protected void extractBytes(Catalog catalog, Connection connection) throws SQLException
    {
        // nothing yet
    }
    @Override
    protected void extractPages(Catalog catalog, Connection connection) throws SQLException
    {
        // nothing yet
    }
}
