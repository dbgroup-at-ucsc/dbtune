/* **************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                         *
 *                                                                              *
 *   Licensed under the Apache License, Version 2.0 (the "License");            *
 *   you may not use this file except in compliance with the License.           *
 *   You may obtain a copy of the License at                                    *
 *                                                                              *
 *       http://www.apache.org/licenses/LICENSE-2.0                             *
 *                                                                              *
 *   Unless required by applicable law or agreed to in writing, software        *
 *   distributed under the License is distributed on an "AS IS" BASIS,          *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 *   See the License for the specific language governing permissions and        *
 *   limitations under the License.                                             *
 * **************************************************************************** */
package edu.ucsc.dbtune.metadata;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Metadata for a table.
 *
 * @author Ivo Jimenez
 */
public class Table extends DatabaseObject
{
    protected Schema schema;
    protected int    type;

    protected List<Column> _columns;
    protected List<Index>  _indexes;

    public static final int REGULAR = 1;
    public static final int MDC     = 2;

    /**
     * Constructor
     *
     * @param name
     *     name of the table
     * @param schema
     *     schema where the new table will be contained
     * @throws SQLException
     *     if a table with the given name is already contained in the schema
     */
    public Table(Schema schema, String name) throws SQLException
    {
        super( name );

        this.type     = REGULAR;
        this._columns = new ArrayList<Column>();
        this._indexes = new ArrayList<Index>();
        this.schema   = schema;

        schema.add(this);
    }

    /**
     * Copy Constructor
     *
     * @param other
     *     object being copied
     */
    protected Table(Table other)
    {
        super(other);

        _columns = new ArrayList<Column>(other._columns);
        _indexes = new ArrayList<Index>(other._indexes);
        type     = other.type;
        schema   = other.schema;
    }

    /**
     * Returns the schema that contains this table.
     *
     * @return
     *     object that contains the table.
     */
    public Schema getSchema()
    {
        return schema;
    }

    /**
     * Adds a column to the table. The position of the column in the table with respect to other 
     * columns is as if the table had. That is, if the table has n columns, the new column will be 
     * placed in the (n+1)th position.
     *
     * @param column
     *     new column being added to the table.
     * @throws SQLException
     *     if column is already contained in the table
     */
    void add(Column column) throws SQLException
    {
        if(_columns.contains(column))
            throw new SQLException("Column " + column + " already in table");

        _columns.add( column );
    }

    /**
     * Adds an index to the table.
     *
     * @param index
     *     new index being added to the table.
     */
    public void add(Index index) throws SQLException
    {
        if(_indexes.contains(index))
            throw new SQLException("Index " + index + " already in table");

        _indexes.add(index);

        // XXX: determine whether or not the properties of an added index have to be checked. For
        //      instance, if an index is already contained and is CLUSTERED, no other index can be
        //      added that is also CLUSTERED. Similarly for PRIMARY/SECONDARY.
    }

    /**
     * Removes an index from the table.
     *
     * @param index
     *     index being removed
     */
    public void remove(Index index) throws SQLException
    {
        _indexes.remove(index);
        // XXX: determine whether or not we need to check the implications of the removal
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
    public boolean contains(Index index)
    {
        return _indexes.contains(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof Table))
            return false;

        Table tbl = (Table) other;

        return schema.getCatalog() == tbl.schema.getCatalog() &&
               schema == tbl.schema &&
               //type == tbl.type &&
               name.equals(tbl.name);

        /*
        for(Column col : _columns)
            if(!tbl._columns.contains(col))
                return false;
        for(Index idx : _indexes)
            if(!tbl._indexes.contains(idx))
                return false;

        return true;
        */
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return 31 * schema.getCatalog().hashCode() * schema.hashCode() * name.hashCode(); //type;

        /*
        for(Column col : _columns)
            hash += col.hashCode();
        for(Index idx : _indexes)
            hash += idx.hashCode();

        return hash;
        */
    }
}
