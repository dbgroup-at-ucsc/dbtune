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

import edu.ucsc.dbtune.util.ToStringBuilder;

/**
 * Represents metadata for table contained in a DBMS.
 */
public abstract class AbstractDatabaseTable implements DatabaseTable
{
    private static final long serialVersionUID = 1L;
    private final int oid;

    private String name;
    // private List<DatabaseColumn> columns;

    public AbstractDatabaseTable( String name )
    {
	this.name = name;
	this.oid  = 0;
    }

    /**
     *
     * @param o
     */
    AbstractDatabaseTable(int o) 
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
     *
     * @return
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
	return new ToStringBuilder<AbstractDatabaseTable>(this)
	    .add("oid", getOid())
	    .toString();
    }
}
