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

import java.sql.SQLException;

/**
 * Interface for the main class of the metadata extraction package
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public interface MetaDataExtractor
{
    /**
     * Given a database connection, it extracts metadata information. The information is comprised 
     * of all the database objects defined in the database that is associated with the connection.
     * 
     * @param connection
     *     object used to obtain metadata for its associated database
     */
    public Catalog extract( DatabaseConnection<?> connection ) throws SQLException;
}
