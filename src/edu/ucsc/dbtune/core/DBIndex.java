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
 * 
 * @param <I>
 */
public interface DBIndex<I extends DBSystem<I>> {
    /**
     * 
     * @return
     */
    double creationCost();

    /**
     * 
     * @return
     */
	String creationText();

    /**
     * 
     * @return
     */
	DatabaseTable baseTable();

    /**
     *
     * @return
     */
	int columnCount();

    /**
     *
     * @param id
     * @return
     * @throws java.sql.SQLException
     */
	DBIndex<I> consDuplicate(int id) throws SQLException;

    @Override
    boolean equals(Object obj);

    /**
     * 
     * @param i
     * @return
     */
	// we only need to use the equals() method of a column
    DatabaseIndexColumn getColumn(int i);
    
    @Override
	int hashCode();

    /**
     * 
     * @return
     */
	int internalId();

    /**
     * 
     * @return
     */
	double megabytes();

    /**
     * 
     * @return
     */
	String toString();
}
