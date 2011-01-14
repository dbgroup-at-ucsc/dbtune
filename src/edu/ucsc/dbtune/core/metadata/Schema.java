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

import java.util.List;
import java.util.ArrayList;

/**
 * A schema is a container of database objects of type tables, indexes, views, etcetera.
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class Schema extends DatabaseObject
{
    protected Catalog catalog;

    protected List<Table> tables;

    static final long serialVersionUID = 0;

    /**
     * default constructor
     */
    public Schema( )
    {
        this.tables = new ArrayList<Table>();
    }

    /**
     * constructs a new schema whose name is given
     *
     * @param name
     *     name of the schema
     */
    public Schema( String name )
    {
        this.name = name;
        this.tables = new ArrayList<Table>();
    }

    /**
     * returns the list of tables that the schema contains
     *
     * @return
     *     List of Table objects
     */
    public List<Table> getTables()
    {
        return new ArrayList<Table>(tables);
    }

    /**
     * adds a table to the schema
     *
     * @param table
     *     new table to add
     */
    public void add( Table table )
    {
        tables.add(table);
        table.setSchema( this );
    }

    /**
     * returns the catalog where the schema is stored
     *
     * @return
     *     the Catalog object
     */
    public Catalog getCatalog()
    {
        return catalog;
    }

    /**
     * assigns the catalog where the schema is stored
     *
     * @param catalog
     *     catalog that contains the schema
     */
    public void setCatalog( Catalog catalog )
    {
        this.catalog = catalog;
    }
}
