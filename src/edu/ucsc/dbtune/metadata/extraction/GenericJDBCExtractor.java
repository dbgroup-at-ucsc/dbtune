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

package edu.ucsc.dbtune.metadata.extraction;

import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

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
     * <p>
     * The user of this method can assume that information for tables, columns and indexes 
     * corresponding to the default schema is (tried to be) retrieved.
     * <p>
     * With default schema we mean whatever is the connection's scope, i.e. whatever table is 
     * visible to the given connection as is. This varies depending on how the connection was 
     * configured and, for a default configuration, on the DBMS/driver being used.
     * <p>
     * This method tries to extract, through the use of JDBC's {@link DatabaseMetaData}, the 
     * information corresponding to names, data types and indexes (primary and secondary). When some 
     * information is not available through it, the corresponding metadata class (from the
     * {@link edu.ucsc.dbtune.metadata} package) is empty. In the worst case, the returned
     * <code>Catalog</code> object is empty.
     * 
     * @param connection
     *     object used to obtain metadata for its associated database
     * @throws SQLException
     *     if the driver corresponding to <code>connection</code> returns <code>null</code> when 
     *     calling the {@link java.sql.Connection#getMetaData} method.
     * @see DatabaseMetaData
     * @see DatabaseMetaData#getTables
     * @see DatabaseMetaData#getColumns
     * @see DatabaseMetaData#getIndexInfo
     */
	public Catalog extract( DatabaseConnection connection )
		throws SQLException
	{
		Map<Integer,Column> indexToColumns;
		Map<String,Schema>  schemaNamesToSchemas;
		List<Table>         tables;
		List<Index>         allIndexes;

		DatabaseMetaData jdbcMetaData;
		ResultSet        rsset;
		Catalog          catalog;
		Schema           schema;
		Column           column;
		Index            index;
		String           schemaName;
		String           columnName;
		String           indexName;
		int              type;
		boolean          isUnique;
		boolean          isClustered;
		boolean          isPrimary = false;

		try
		{
			schemaNamesToSchemas = new HashMap<String,Schema>();

			jdbcMetaData = connection.getJdbcConnection().getMetaData();

			if (jdbcMetaData == null)
			{
				throw new SQLException( "Connection " + connection + " doesn't handle JDBC metadata" );
			}

			String[] tableTypes = {"TABLE"};

			rsset = jdbcMetaData.getTables( null, null, "%", tableTypes );

			catalog = new Catalog();

			while (rsset.next())
			{
				catalog.setName(rsset.getString("TABLE_CAT"));

				schemaName = rsset.getString("TABLE_SCHEM");

				if(schemaName == null) {
					schemaName = "default";
				}

				schema = schemaNamesToSchemas.get(schemaName);

				if(schema == null) {
					schema = new Schema(schemaName);
					schemaNamesToSchemas.put(schemaName,schema);
					catalog.add(schema);
				}

				schema.add(new Table(rsset.getString("TABLE_NAME")));
			}

			rsset.close();

			allIndexes = new ArrayList<Index>();

			for(Schema sch : catalog.getSchemas()) {

				tables = sch.getTables();

				for(Table table : tables) {

					rsset = jdbcMetaData.getColumns( null, sch.getName(), table.getName(), "%" );

					while (rsset.next())
					{
						columnName = rsset.getString("COLUMN_NAME");
						type       = rsset.getInt("DATA_TYPE");

						table.add(new Column( columnName, type ));
					}

					rsset.close();

					rsset          = jdbcMetaData.getIndexInfo( null, sch.getName(), table.getName(), false, true );
					indexToColumns = new HashMap<Integer,Column>();
					indexName      = "";
					index          = null;

					while (rsset.next())
					{
						type = rsset.getShort("TYPE");

						if (type == DatabaseMetaData.tableIndexStatistic)
						{
							table.setPages( rsset.getInt("PAGES") );
							table.setCardinality( rsset.getInt("CARDINALITY") );
						}
						else
						{
							if (!indexName.equals(rsset.getString("INDEX_NAME")))
							{
								if (index != null)
								{
									for (int i = 0; i < indexToColumns.size(); i++)
									{
										index.add(indexToColumns.get(i+1));
									}
								}

								type = rsset.getShort("TYPE");

								if (type == DatabaseMetaData.tableIndexClustered)
								{
									isClustered = true;
								}
								else
								{
									isClustered = false;
								}

								isUnique       = !rsset.getBoolean("NON_UNIQUE");
								indexName      = rsset.getString("INDEX_NAME");
								index          = new Index(indexName, table, isPrimary, isClustered, isUnique);
								indexToColumns = new HashMap<Integer,Column>();

								table.add(index);
								allIndexes.add(index);
							}

							columnName = rsset.getString("COLUMN_NAME");
							column     = table.findColumn(columnName);

							if (column == null)
							{
								throw new SQLException("Column " + columnName + " not in " + table);
							}

							indexToColumns.put(rsset.getInt("ORDINAL_POSITION"), column);
						}
					}

					rsset.close();
				}

				sch.setBaseConfiguration(new Configuration(allIndexes));
			}
		}
		catch (Exception e)
		{
			throw new SQLException(e);
		}

		return catalog;
	}
}
