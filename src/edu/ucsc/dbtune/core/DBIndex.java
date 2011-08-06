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

package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.core.metadata.Table;
import java.sql.SQLException;

/**
 * an immutable type which represents the Index concept, which is defined as
 * data structure that improves the speed of data retrieval operations on a database table.
 */
public interface DBIndex {
    /**
     * @return the cost required to create this index.
     */
    double creationCost(); // #44: Index.getCreationCost(); Index.setCreationCost();

    /**
     * @return create index statement.
     */
	String creationText(); // #44: Index.getCreateStatement();

    /**
     * @return the base table where this index will be used.
     */
	Table baseTable(); // #44: Index.getTable();

    /**
     * @return the number of columns of the schema where this is index is part of.
     */
	int columnCount(); // #44: Index.size();

    /**
     * duplicate an index in a type-safe fashion.
     *
     * @param id id of the duplicate
     * @return the duplicate of {@code this} index.
     * @throws java.sql.SQLException
     *      unable to duplicate an index for the stated reasons.
     */
	DBIndex consDuplicate(int id) throws SQLException; // #44: drop

    @Override
    boolean equals(Object obj); // #44: Index.equals();

    /**
     * @param i position of col
     * @return a database column at position {@code i}.
     */
    DatabaseColumn getColumn(int i); // #44: Index.get(i);
    
    @Override
	int hashCode(); // #44: Index.hashCode();

    /**
     * @return the internal Id of this index.
     */
	int internalId(); // #44: DatabaseObject.getId()

    /**
     * @return the size of the index.
     */
	double megabytes(); // #44: DatabaseObject.getMegaBytes()

    @Override   // this encourages developer to provide a human-readable string.
	String toString(); // #44: Index.toString();
}
