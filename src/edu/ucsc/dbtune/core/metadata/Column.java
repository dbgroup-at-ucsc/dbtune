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

/**
 * POJO for storing column metadata
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class Column extends DatabaseObject
{
    protected Table table;
    protected int     type;
    protected boolean isNull;
    protected boolean isDefault;
    protected String  defaultValue;
    protected int     size;

    static final long serialVersionUID = 0;

    /**
     * constructor
     */
    public Column( String name )
    {
        this(name, -1);
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
    public Column( String name, int type )
    {
        this.name         = name;
        this.type         = type;
        this.table        = null;
        this.isNull       = true;
        this.isDefault    = true;
        this.defaultValue = "";

        // TODO check that type is valid (a value from SQLTypes)
    }

    /**
     * Assigns the table that contains this column.
     *
     * @param table
     *     object that contains the column.
     */
    public void setTable( Table table )
    {
        this.table = table;
    }

    /**
     * Assigns the size of the column. This is relevant for character data-types.
     *
     * @param isNull
     *     indicates if column can have null values
     */
    public void setSize( int size )
    {
        this.size = size;
    }

    /**
     * Assigns the type of the column. The type should be one of the values defined in SQLTypes.
     *
     * @param int
     *     type of the column
     */
    protected void setDataType( int type )
    {
        // TODO check that is one of the types defined in SQLTypes
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
        if( type == SQLTypes.VARCHAR ||
                type == SQLTypes.CHARACTER ||
                type == SQLTypes.NCHAR ||
                type == SQLTypes.NUMERIC ||
                type == SQLTypes.DECIMAL ||
                type == SQLTypes.FLOAT )
        {
            return size;
        }

        return SQLTypes.getSize( type );
    }

    /**
     * Returns the one-based position of the column with respect to its containing table.
     *
     * @return
     *     the ordinal position of this column
     */
    public int getOrdinalPosition()
    {
        return table.columns.indexOf(this) + 1;
    }
}
