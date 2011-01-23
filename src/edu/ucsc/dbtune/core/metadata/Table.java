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
public class Table extends AbstractDatabaseTable
{
    protected Schema schema;

    protected List<Column> columns;
    protected List<Index>  indexes;

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
        indexes = new ArrayList<Index>();
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

    /**
     * Adds a column to the table. The position of the column in the table with respect to other 
     * columns is as if the table had. That is, if the table has n columns, the new column will be 
     * placed in the (n+1)th position.
     *
     * @param column
     *     new column being added to the table.
     */
    public void add( Index index )
    {
        indexes.add( index );

        index.setTable( this );
    }

    /**
     * Returns the list of columns that are inside the table.
     *
     * @return columns from table
     */
    public List<Column> getColumns()
    {
        return new ArrayList<Column>(columns);
    }

    /**
     * Returns the list of indexes that are inside the table.
     *
     * @return indexes from table
     */
    public List<Index> getIndexes()
    {
        return new ArrayList<Index>(indexes);
    }

    /**
     * Finds a column whose name matches the given argument.
     *
     * @param name
     *     name of the object that is searched for in <code>this</code> table.
     * @return
     *     the column that has the given name; null if not found
     */
    public Column findColumn(String name)
    {
        return (Column) DatabaseObject.findByName(new ArrayList<DatabaseObject>(columns),name);
    }

    /**
     * Finds an index whose name matches the given argument.
     *
     * @param name
     *     name of the object that is searched for in <code>this</code> table.
     * @return
     *     the index that has the given name; null if not found
     */
    public Index findIndex(String name)
    {
        return (Index) DatabaseObject.findByName(new ArrayList<DatabaseObject>(indexes),name);
    }

    /**
     * Whether or not the given column is contained
     *
     * @param column
     *     object that is searched for in <code>this</code>
     * @return
     *     <code>true</code> if found; <code>false</code> otherwise
     */
    public boolean contains( Column column )
    {
        return columns.contains(column);
    }

    /**
     * Whether or not the given index is contained
     *
     * @param index
     *     object that is searched for in <code>this</code>
     * @return
     *     <code>true</code> if found; <code>false</code> otherwise
     */
    public boolean contains( Index index )
    {
        return indexes.contains(index);
    }
}
