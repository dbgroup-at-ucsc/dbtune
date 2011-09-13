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
import static edu.ucsc.dbtune.metadata.SQLTypes.isValidType;

/**
 * POJO for representing column metadata
 *
 * @author Ivo Jimenez
 */
public class Column extends DatabaseObject
{
    Table   table;
    int     type;
    boolean isNull;
    boolean isDefault;
    String  defaultValue;

    /**
     * Creates a column with the given name and type. The type should be one of the values defined 
     * in SQLTypes.
     *
     * @param name
     *     name assigned to the column.
     * @param type
     *     data type
     * @param table
     *     table where the new column will be contained
     * @throws SQLException
     *     if a column with the given name is already contained in the table
     */
    public Column(Table table, String name, int type) throws SQLException
    {
        super(name);

        this.table        = null;
        this.isNull       = true;
        this.isDefault    = true;
        this.defaultValue = "";
        this.table        = table;

        setDataType(type);

        table.add(this);
    }

    /**
     * copy constructor
     *
     * @param other
     *     column object being copied
     */
    protected Column(Column other) throws SQLException
    {
        this(other.table, other.name, other.type);

        this.isNull       = other.isNull;
        this.isDefault    = other.isDefault;
        this.defaultValue = other.defaultValue;
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
            throw new RuntimeException("Invalid data type " + type);

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
    @Override
    public long getBytes()
    {
        if (type == SQLTypes.VARCHAR ||
            type == SQLTypes.CHARACTER ||
            type == SQLTypes.NCHAR ||
            type == SQLTypes.NUMERIC ||
            type == SQLTypes.DECIMAL ||
            type == SQLTypes.FLOAT)
        {
            return bytes;
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
        return table._columns.indexOf(this) + 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Column))
            return false;

        Column col = (Column) other;

        return
            table.getSchema().getCatalog() == col.table.getSchema().getCatalog() &&
            table.getSchema() == col.table.getSchema() &&
            table == col.table &&
            name.equals(col.name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return 
            31 * 
            table.getSchema().getCatalog().hashCode() * 
            table.getSchema().hashCode() * 
            table.getName().hashCode();
    }
}
