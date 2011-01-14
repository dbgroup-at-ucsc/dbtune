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
import edu.ucsc.dbtune.core.metadata.Schema;
import edu.ucsc.dbtune.core.metadata.Table;

import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

/**
 * Extractor that uses JDBC's DatabaseMetadata class to obtain basic metadata information.
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class GenericJDBCExtractor implements MetaDataExtractor
{
    /**
     * Given a database connection, it extracts metadata information. The information is comprised 
     * of all the database objects defined in the database that is associated with the connection.
     * 
     * @param connection
     *     object used to obtain metadata for its associated database
     */
    public Catalog extract( DatabaseConnection connection )
	throws SQLException
    {
	DatabaseMetaData jdbcMetaData;
	ResultSet        rs;
	Catalog          catalog;
	Schema           schema;

        catalog = new Catalog();
        schema  = new Schema();

	catalog.add( schema );

        jdbcMetaData = connection.getJdbcConnection().getMetaData();

        if (jdbcMetaData == null)
        {
            throw new SQLException("Connection " + connection + " doesn't handle JDBC metadata" );
        }

        String[] tableTypes = {"TABLE"};

        rs = jdbcMetaData.getTables(null,null,"%",tableTypes);

        while (rs.next())
        {
            schema.add( new Table( rs.getString(3) ) );
        }

        rs.close();

	return catalog;
    }
}
