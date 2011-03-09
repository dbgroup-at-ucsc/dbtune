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

package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.core.metadata.DatabaseObject;

/**
 * Represents metadata for table contained in a DBMS.
 */
public abstract class AbstractDatabaseTable
    extends DatabaseObject
    implements DatabaseTable
{
    private final int oid;

    public AbstractDatabaseTable( String name )
    {
        this.name = name;
        this.oid  = 0;
    }

    /**
     *
     * @param o
     */
    public AbstractDatabaseTable(int o) 
    {
        oid = o;
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof AbstractDatabaseTable
            && ((AbstractDatabaseTable) other).getOid() == getOid();
    }

    /**
     * @return
     *     object id
     */
    public int getOid()
    {
        return oid;
    }

    @Override
    public int hashCode()
    {
        return 34 * getOid();
    }

    @Override
    public String toString() 
    {
        return name;
    }
}
