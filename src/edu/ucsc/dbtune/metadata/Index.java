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

package edu.ucsc.dbtune.metadata;

import java.util.List;
import java.util.ArrayList;

/**
 * Represents the abstraction for Index metadata
 *
 * @author Ivo Jimenez
 */
public class Index extends DatabaseObject
{
    public static final int     UNKNOWN        = 0;
    public static final int     B_TREE         = 1;
    public static final int     BITMAP         = 2;
    public static final int     HASH           = 3;
    public static final int     REVERSIBLE     = 4;
    public static final int     NON_REVERSIBLE = 5;
    public static final int     SYNCHRONIZED   = 6;
    public static final boolean PRIMARY        = true;
    public static final boolean SECONDARY      = false;
    public static final boolean CLUSTERED      = true;
    public static final boolean UNCLUSTERED    = false;
    public static final boolean UNIQUE         = true;
    public static final boolean NON_UNIQUE     = false;

    protected Table         table;
    protected List<Column>  columns;
    protected List<Boolean> descending;
    protected int           type;
    protected int           scanOption;
    protected double        updateCost;
    protected boolean       unique;
    protected boolean       primary;
    protected boolean       clustered;
    protected boolean       materialized;

    /**
     * Creates an empty index.
     *
     * @param name
     *     name of the index
     * @param table
     *     table over which the index will be defined.
     * @param primary
     *     whether or not the index is primary
     * @param unique
     *     whether the index is unique or not.
     * @param clustered
     *     whether the corresponding table is clustered on this index
     */
    public Index(String name, Table table, boolean primary, boolean unique, boolean clustered)
    {
        super(name);

        this.table      = table;
        this.type       = UNKNOWN;
        this.primary    = primary;
        this.unique     = unique;
        this.columns    = new ArrayList<Column>();
        this.clustered  = clustered;
        this.descending = new ArrayList<Boolean>();
        this.scanOption = NON_REVERSIBLE;
        this.updateCost = 0.0;
    }

    /**
     * Copy constructor
     *
     * @param other
     *     other index being copied into the new one
     */
    public Index(Index other)
    {
        super(other);

        this.table        = other.table;
        this.columns      = other.columns;
        this.type         = other.type;
        this.unique       = other.unique;
        this.primary      = other.primary;
        this.clustered    = other.clustered;
        this.materialized = other.materialized;
        this.descending   = other.descending;
        this.scanOption   = other.scanOption;
        this.updateCost   = other.updateCost;
    }

    /**
     * Creates an index from the given columns, primary, uniqueness and clustering values.
     *
     * @param name
     *     name of the index
     * @param columns
     *     columns that will define the index
     * @param primary
     *     whether or not the index is primary
     * @param clustered
     *     whether the corresponding table is clustered on this index
     * @throws Exception
     *     if column list empty or not all of the columns in the list correspond to the same table.
     */
    public Index(String name, List<Column> columns, boolean primary, boolean unique, boolean clustered)
        throws Exception
    {
        super(name);

        if (columns.size() == 0)
        {
            throw new Exception("Column list should have at least one element");
        }

        this.columns = new ArrayList<Column>();
        this.table   = columns.get(0).getTable();

        this.columns.add(columns.get(0));

        for (int i = 1; i < columns.size(); i++)
        {
            if (this.table != columns.get(i).getTable())
            {
                throw new Exception("Columns from different tables");
            }

            this.columns.add(columns.get(i));
        }

        this.type         = UNKNOWN;
        this.primary      = primary;
        this.unique       = unique;
        this.clustered    = clustered;
        this.descending   = new ArrayList<Boolean>();
        this.scanOption   = NON_REVERSIBLE;
        this.updateCost   = 0.0;
    }

    /**
     * Assigns the table that contains this index.
     *
     * @param table
     *     object that contains the index.
     */
    public void setTable(Table table)
    {
        this.table = table;
    }

    /**
     * Sets the type of index. Either PRIMARY, CLUSTERED or SECONDARY.
     *
     * @param type
     *     one of the available fields.
     */
    public void setType(int type)
    {
        switch( type )
        {
            case UNKNOWN:
            case B_TREE:
            case BITMAP:
                break;
            default:
                throw new RuntimeException("Invalid type " + type);
        }

        this.type = type;
    }

    /**
     * Returns the type of the index.
     *
     * @return
     *     type of this index
     */
    public int getType()
    {
        return type;
    }

    /**
     * Sets the value of the <code>unique</code> property of this index.
     *
     * @param unique
     *     value to be assigned
     */
    public void setUnique(boolean unique)
    {
        this.unique = unique;
    }

    /**
     * Returns the scan option of the index.
     *
     * @return
     *     scan option of this index
     */
    public int getScanOption()
    {
        return scanOption;
    }

    /**
     * Sets the value of the <code>scanOption</code> property of this index.
     *
     * @param scanOption
     *     value to be assigned
     */
    public void setScanOption(int scanOption)
    {
        this.scanOption = scanOption;
    }

    /**
     * Whether or not this index is unique.
     *
     * @return
     *     <code>true</code> if unique; <code>false</code> otherwise
     */
    public boolean isUnique()
    {
        return unique;
    }

    /**
     * Sets the value of the <code>primary</code> property of this index.
     *
     * @param primary
     *     value to be assigned
     */
    public void setPrimary(boolean primary)
    {
        this.primary = primary;
    }

    /**
     * Whether or not this index corresponds to the table's primary key.
     *
     * @return
     *     <code>true</code> if table's primary key contains the same columns as this index; 
     *     <code>false</code> otherwise
     */
    public boolean isPrimary()
    {
        return primary;
    }

    /**
     * Sets the value of the <code>clustered</code> property of this index.
     *
     * @param clustered
     *     whether or not the index is clustered
     */
    public void setClustered(boolean clustered)
    {
        this.clustered = clustered;
    }

    /**
     * Whether or not this index defines the corresponding's table clustering. For indexes that are 
     * materialized, only one of them can return <code>true</code>.
     *
     * @return
     *     <code>true</code> if table's clustered on it; <code>false</code> otherwise
     */
    public boolean isClustered()
    {
        return clustered;
    }

    /**
     * Sets the value of the <code>materialized</code> property of this index.
     *
     * @param materialized
     *     value to be assigned
     */
    public void setMaterialized(boolean materialized)
    {
        this.materialized = materialized;
    }

    /**
     * Whether or not this index is materialized, i.e. exists in the database and not only as an 
     * in-memory object.
     *
     * @return
     *     <code>true</code> if index is materialized; <code>false</code> otherwise
     */
    public boolean isMaterialized()
    {
        return materialized;
    }

    /**
     * adds a new column to the index. If the column is already contained it does nothing, i.e.  
     * repetitions aren't allowed
     *
     * @param column
     *     new column to be inserted to the sequence
     */
    public void add(Column column)
        throws Exception
    {
        if (table != column.getTable())
        {
            throw new Exception("Table " + table + " doesn't contain column " + column);
        }

        if (!contains(column))
        {
            columns.add(column);
            descending.add(true);
        }
    }

    /**
     * Whether or not the given column is contained
     *
     * @param column
     *     object that is searched for in <code>this</code>
     * @return
     *     <code>true</code> if found; <code>false</code> otherwise
     */
    public boolean contains(Column column)
    {
        return columns.contains(column);
    }

    /**
     * Returns the list of columns that are inside the index.
     *
     * @return columns within index
     */
    public List<Column> getColumns()
    {
        return new ArrayList<Column>(columns);
    }

    /**
     * returns the number of columns contained in the index
     *
     * @return
     *     number of columns
     */
    public int size()
    {
        return columns.size();
    }

    /**
     * returns the element contained at the position given
     *
     * @param  i
     *     index of the column to be retrieved
     * @return
     *     the corresponding column metadata if the index is a valid index; <code>null</code> 
     *     otherwise
     */
    public Column get(int i)
    {
        if (i < columns.size())
        {
            return columns.get(i);
        }
        else
        {
            return null;
        }
    }
    public Column getColumn(int i) {
        if (i < columns.size())
        {
            return columns.get(i);
        }
        else
        {
            return null;
        }
    }

    /**
     * Returns the corresponding table object.
     *
     * @return
     *      table that contains this index
     */
    public Table getTable()
    {
        return table;
    }

    /**
     * Returns the list of descending values of the index
     *
     * @return
     *     <code>true</code> if descending; <code>false</code> if ascending
     */
    public List<Boolean> getDescending()
    {
        return descending;
    }

    /**
     * @return create index statement.
     */
    public String getCreateStatement() {
        throw new RuntimeException("Not implemented here");
    }

    /**
     * Returns the hash code value for this index. The hash code of a index is defined to be the sum 
     * of the hash codes of the <code>Set<Column></code> object, plus the index type,uniqueness, 
     * clustered and primary properties, as well as the name.
     * 
     * This ensures that s1.equals(s2) implies that s1.hashCode()==s2.hashCode() for any two index 
     * objects s1 and s2, as required by the general contract of Object.hashCode()
     *
     * @return
     *     integer representing the hashCode for the Index
     */
    @Override
    public int hashCode()
    {
        int hash;

        if (id != -1)
        {
            return 1 * 31 + (new Long(id)).hashCode();
        }

        hash = 1;

        hash = hash * 31 + table.hashCode();

        for (Column col : columns)
        {
            hash = hash * 31 + col.hashCode();
        }

        hash = hash *31 + (new Integer(type)).hashCode();
        hash = hash *31 + (new Boolean(primary)).hashCode();
        hash = hash *31 + (new Boolean(clustered)).hashCode();
        hash = hash *31 + (new Boolean(unique)).hashCode();

        return hash;
    }

    /**
     * an Index object is equal to other if:
     *   - both refer to the same Table
     *   - both contain the same set of columns in the same order
     *   - both are of the same type
     *
     * @param other
     *     object that is compared to this one
     * @return
     *     true if equal, false if not
     */
    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }

        if (!(other instanceof Index))
        {
            return false;
        }

        Index idx = (Index) other;

        if (id != -1 && idx.getId() != -1)
        {
            return id == idx.getId();
        }

        if (name != idx.getName())
        {
            return false;
        }

        if (clustered != idx.isClustered())
        {
            return false;
        }

        if (primary != idx.isPrimary())
        {
            return false;
        }

        if (unique != idx.isUnique())
        {
            return false;
        }

        if (table != idx.getTable())
        {
            return false;
        }

        if (idx.size() != columns.size()) 
        {
            return false;
        }

        if (idx.getType() != type)
        {
            return false;
        }

        for (int i = 0; i < idx.size(); i++)
        {
            if (!idx.get(i).equals(columns.get(i)))
            {
                return false;
            }
        }

        return true;
    }
}
