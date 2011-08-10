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

import static edu.ucsc.dbtune.core.metadata.SQLTypes.isValidType;

/**
 * POJO for representing column metadata
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class Column extends DatabaseObject
{
    Table   table;
    int     type;
    boolean isNull;
    boolean isDefault;
    String  defaultValue;
    int     size;

    /**
     * Creates a column with the given number (with respect to its table).
     *
     * @param attNum
     *     attribute number with respect to its containing table
     * @deprecated
     *     see issue #53
     */
    @Deprecated
    public Column(int attNum)
    {
        // XXX: this constructor should be dropped when issue #53 is fixed
        this("",SQLTypes.INT);

        this.id = attNum;
    }

    /**
     * Creates a column with the given name and type. The type should be one of the values defined 
     * in SQLTypes.
     *
     * @param name
     *     name assigned to the column.
     * @param type
     *     data type
     */
    public Column(String name, int type)
    {
        super(name);

        this.table        = null;
        this.isNull       = true;
        this.isDefault    = true;
        this.defaultValue = "";

        setDataType(type);
    }

    /**
     * copy constructor
     *
     * @param other
     *     column object being copied
     */
    public Column(Column other)
    {
        this(other.name, other.type);

        this.table        = other.table;
        this.isNull       = other.isNull;
        this.isDefault    = other.isDefault;
        this.defaultValue = other.defaultValue;
    }

    /**
     * Assigns the table that contains this column.
     *
     * @param table
     *     object that contains the column.
     */
    public void setTable(Table table)
    {
        this.table = table;
    }

    /**
     * Assigns the size of the column. This is relevant for character-valued datatypes.
     *
     * @param size
     *     size of the column
     */
    public void setSize(int size)
    {
        this.size = size;
    }

    /**
     * Assigns the type of the column. The type should be one of the values defined in SQLTypes.
     *
     * @param type
     *     type of the column
     */
    protected void setDataType(int type)
    {
        if (!isValidType(type))
        {
            throw new RuntimeException("Invalid data type " + type);
        }

        this.type = type;
    }

    /**
     * Returns the table that contains this column.
     *
     * @return
     *     table containing the column
     */
    public Table getTable()
    {
        return table;
    }

    /**
     * The data type of the column
     *
     * @return
     *     one of the values from SQLTypes
     */
    public int getDataType()
    {
        return type;
    }

    /**
     * Returns the size of the column in bytes.
     *
     * @return
     *     size of the columns in bytes
     */
    public int getSize()
    {
        if (type == SQLTypes.VARCHAR ||
            type == SQLTypes.CHARACTER ||
            type == SQLTypes.NCHAR ||
            type == SQLTypes.NUMERIC ||
            type == SQLTypes.DECIMAL ||
            type == SQLTypes.FLOAT)
        {
            return size;
        }

        return SQLTypes.getSize(type);
    }

    /**
     * Returns the one-based position of the column with respect to its containing table.
     *
     * @return
     *     the ordinal position of this column
     */
    public int getOrdinalPosition()
    {
        if (table == null) { // XXX: remove when issue #53 is fixed
            return (int)id; 
        }

        return table._columns.indexOf(this) + 1;
    }
}
