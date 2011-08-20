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

import java.sql.SQLException;
import java.sql.Connection;

/**
 * Metadata extractor for DB2.
 * <p>
 * This class assumes a DB2 system version ?? or greater is on the backend and connections
 * created using the postgres' JDBC driver (type 4) version ?? or greater.
 *
 * @author Ivo Jimenez
 */
public class DB2Extractor extends GenericJDBCExtractor
{
    @Override
    protected void extractCatalog(Catalog catalog, Connection connection) throws SQLException
    {
        throw new SQLException("Not implemented yet");
    }
    @Override
    protected void extractSchemas(Catalog catalog, Connection connection) throws SQLException
    {
        throw new SQLException("Not implemented yet");
    }
    @Override
    protected void extractBytes(Catalog catalog, Connection connection) throws SQLException
    {
        throw new SQLException("Not implemented yet");
    }
    @Override
    protected void extractPages(Catalog catalog, Connection connection) throws SQLException
    {
        throw new SQLException("Not implemented yet");
    }
    @Override
    protected void extractCardinality(Catalog catalog, Connection connection) throws SQLException
    {
        throw new SQLException("Not implemented yet");
    }
}
