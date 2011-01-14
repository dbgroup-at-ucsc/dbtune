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

import java.sql.SQLException;

/**
 * an immutable type which represents the Index concept, which is defined as
 * data structure that improves the speed of data retrieval operations on a database table.
 */
public interface DBIndex {
    /**
     * @return the cost required to create this index.
     */
    double creationCost();

    /**
     * @return create index statement.
     */
	String creationText();

    /**
     * @return the base table where this index will be used.
     */
	DatabaseTable baseTable();

    /**
     * @return the number of columns of the schema where this is index is part of.
     */
	int columnCount();

    /**
     * duplicate an index in a type-safe fashion.
     *
     * @param id id of the duplicate
     * @return the duplicate of {@code this} index.
     * @throws java.sql.SQLException
     *      unable to duplicate an index for the stated reasons.
     */
	DBIndex consDuplicate(int id) throws SQLException;

    @Override
    boolean equals(Object obj);

    /**
     * @param i position of col
     * @return a database column at position {@code i}.
     */
	// we only need to use the equals() method of a column
    DatabaseColumn getColumn(int i);
    
    @Override
	int hashCode();

    /**
     * compares qualified names.
     * @param name
     *    qualied name to be used in the comparison.
     * @return
     *      {@code true} if both qualified names are the same, false
     *      otherwise.
     */
    boolean isOn(DatabaseTable name);

    /**
     * @return the internal Id of this index.
     */
	int internalId();

    /**
     * @return the size of the index.
     */
	double megabytes();

    @Override   // this encourages developer to provide a human-readable string.
	String toString();
}
