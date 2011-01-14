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
 *  ****************************************************************************
 */

package edu.ucsc.dbtune.core.metadata;

import edu.ucsc.dbtune.core.DatabaseTable;
import edu.ucsc.dbtune.util.Objects;

public class DB2QualifiedName implements DatabaseTable {
	private static final long serialVersionUID = 1L;
	
	private final String dbName;
	private final String schemaName;
	private final String name;

    /**
     * construct a qualified name instance.
     * @param dbName
     *      database name
     * @param schemaName
     *      schema name
     * @param name
     *      table name
     */
	DB2QualifiedName(String dbName, String schemaName, String name) {
		this.dbName     = dbName;
		this.schemaName = schemaName;
		this.name       = name;
	}

    /**
     *
     * @return name of db2 database
     */
    public String getDbName() {
        return dbName;
    }

    /**
     *
     * @return name of db2 table (i.e., name of db2 qualified name)
     */
    public String getName() {
        return name;
    }

    /**
     * @return db2 schema name
     */
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getDbName(), getSchemaName(), getName());
    }

    public boolean equals(Object o) {
		if (!(o instanceof DB2QualifiedName))
			return false;
		DB2QualifiedName other = (DB2QualifiedName) o;
		return getDbName().equals(other.getDbName())
               && getSchemaName().equals(other.getSchemaName())
               && getName().equals(other.getName());
	}

    @Override
	public String toString() {
        // todo(Huascar) can we use this ToStringBuilder here? hmm...
        return String.format("%s.%s.%s", getDbName(), getSchemaName(), getName());
	}
}
