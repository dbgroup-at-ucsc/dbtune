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

import edu.ucsc.dbtune.core.DBIndexSet;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class DB2ReifiedTypes {
    private DB2ReifiedTypes(){}

    /**
     * A trick to bypass the whole reifying process done by Java and the return
     * a reified DB2IndexSet.
     */
    public static class DB2IndexSet extends DBIndexSet<DB2System> {
        private static final long serialVersionUID = DBIndexSet.serialVersionUID;
    }

}
