/*
 ******************************************************************************
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
 ******************************************************************************/
package edu.ucsc.dbtune.core.metadata;

import java.util.List;
import java.util.ArrayList;

/**
 * Metadata for a table.
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class Table extends DatabaseObject
{
    protected Schema schema;
    private   String schemaName; // XXX: remove after fix of issue #53
    private   String dbName;     // XXX: remove after fix of issue #53

    protected List<Column> _columns;
    protected List<Index>  _indexes;

    /**
     * Constructor
     *
     * @param id
     *     id of the table object
     */
    public Table( long id )
    {
        super( id );

        dbName     = "";
        schemaName = "";
        _columns   = new ArrayList<Column>();
        _indexes   = new ArrayList<Index>();
    }

    /**
     * Constructs a table that corresponds to the given schema and database names.
     *
     * @param dbName
     *      database name
     * @param schemaName
     *      schema name
     * @param name
     *      table name
     * @deprecated
     *      see issue #53
     */
    @Deprecated
    public Table(String dbName, String schemaName, String name) {
        // XXX: this constructor should be dropped when issue #53 is fixed
        super(name);
        this.dbName     = dbName;
        this.schemaName = schemaName;
    }

    /**
     * Constructor
     *
     * @param name
     *     name of the table
     */
    public Table( String name )
    {
        super( name );

        id         = -1;
        dbName     = "";
        schemaName = "";
        _columns   = new ArrayList<Column>();
        _indexes   = new ArrayList<Index>();
    }

    /**
     * Copy Constructor
     *
     * @param other
     *     object being copied
     */
    public Table( Table other )
    {
        super( other );

        _columns = other._columns;
        _indexes = other._indexes;
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
        _columns.add( column );

        column.setTable( this );
    }

    /**
     * Adds a index to the table. The position of the index in the table with respect to other 
     * indexes is as if the table had. That is, if the table has n indexes, the new index will be 
     * placed in the (n+1)th position.
     *
     * @param index
     *     new index being added to the table.
     */
    public void add( Index index )
    {
        _indexes.add( index );

        index.setTable( this );
        // XXX: determine whether or not the properties of an added index have to be checked. For
        //      instance, if an index is already contained and is CLUSTERED, no other index can be
        //      added that is also CLUSTERED. Similarly for PRIMARY/SECONDARY.
    }

    /**
     * Returns the list of columns that are inside the table.
     *
     * @return _columns from table
     */
    public List<Column> getColumns()
    {
        return new ArrayList<Column>(_columns);
    }

    /**
     * Returns the list of indexes that are inside the table.
     *
     * @return indexes from table
     */
    public List<Index> getIndexes()
    {
        return new ArrayList<Index>(_indexes);
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
        return (Column) DatabaseObject.findByName(new ArrayList<DatabaseObject>(_columns),name);
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
        return (Index) DatabaseObject.findByName(new ArrayList<DatabaseObject>(_indexes),name);
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
        return _columns.contains(column);
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
        return _indexes.contains(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        // XXX: drop as part of issue #53
        if(id != -1) {
            return super.hashCode();
        } else {
            return 34 * dbName.hashCode() * schemaName.hashCode() * name.hashCode();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        // XXX: drop as part of issue #53
        if (!(o instanceof Table))
			return false;

        if(id != -1) {
            return super.equals(o);
        }

        Table other = (Table) o;

        return dbName.equals(other.dbName)
               && schemaName.equals(other.schemaName)
               && name.equals(other.name);
	}
}
