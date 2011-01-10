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

package edu.ucsc.dbtune.core.metadata;

import edu.ucsc.dbtune.core.AbstractDatabaseTable;

import java.util.List;
import java.util.ArrayList;

/**
 * Metadata for a table
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public abstract class Table extends AbstractDatabaseTable
{
    private static final long serialVersionUID = 1L;

    protected Schema schema;

    protected List<Column> columns;

    // how do we represent a PK? Index or List<Column>

    /**
     * Constructor
     *
     * @param name
     *     name of the table
     */
    public Table( String name )
    {
	super( name );

	columns = new ArrayList<Column>();
    }

    /**
     * Assigns the schema that contains this table.
     *
     * @param schema
     *     object that contains the table.
     */
    public void setSchema( Schema schema )
    {
        this.schema = schema;
    }

    /**
     * Adds a column to the table. The position of the column in the table with respect to other 
     * columns is as if the table had. That is, if the table has n columns, the new column will be 
     * placed in the (n+1)th position.
     *
     * @param column
     *     new column being added to the table.
     */
    public void add( Column column )
    {
	columns.add( column );

	column.setTable( this );
    }
}
