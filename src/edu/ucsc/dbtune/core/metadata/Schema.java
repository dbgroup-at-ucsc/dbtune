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
 * A schema is a container of database objects, regularly tables, indexes, views, etcetera.
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class Schema extends DatabaseObject
{
    protected List<Table> _tables;

    protected Catalog       _catalog;
    protected Configuration _baseConfiguration;

    /**
     * default constructor
     */
    public Schema()
    {
        super();

        _tables  = new ArrayList<Table>();
    }

    /**
     * copy constructor
     *
     * @param other
     *     other schema being copied
     */
    public Schema(Schema other)
    {
        super(other);

        _tables = other._tables;
        _baseConfiguration = other._baseConfiguration;
    }

    /**
     * constructs a new schema whose name is given
     *
     * @param name
     *     name of the schema
     */
    public Schema(String name)
    {
        super(name);

        _tables = new ArrayList<Table>();
    }

    /**
     * returns the list of _tables that the schema contains
     *
     * @return
     *     List of Table objects
     */
    public List<Table> getTables()
    {
        return new ArrayList<Table>(_tables);
    }

    /**
     * adds a table to the schema
     *
     * @param table
     *     new table to add
     */
    public void add(Table table)
    {
        _tables.add(table);
        table.setSchema(this);
    }

    /**
     * returns the catalog where the schema is stored
     *
     * @return
     *     the Catalog object
     */
    public Catalog getCatalog()
    {
        return _catalog;
    }

    /**
     * assigns the catalog where the schema is stored
     *
     * @param catalog
     *     catalog that contains the schema
     */
    public void setCatalog(Catalog catalog)
    {
        _catalog = catalog;
    }

    /**
     * Assigns the schema's base configuration
     *
     * @param baseConfiguration
     *     configuration corresponding to the schema
     */
    public void setBaseConfiguration( Configuration baseConfiguration )
    {
        _baseConfiguration = baseConfiguration;
    }

    /**
     * Returns the schema's base configuration
     *
     * @return
     *     configuration corresponding to the schema
     */
    public Configuration getBaseConfiguration()
    {
        return _baseConfiguration;
    }

    /**
     * Finds the table whose name matches the given argument.
     *
     * @param name
     *     name of the object that is searched for in <code>this</code> schema.
     * @return
     *     the table that has the given name; {@code null} if not found
     */
    public Table findTable(String name)
    {
        return (Table) DatabaseObject.findByName(new ArrayList<DatabaseObject>(_tables),name);
    }

    /**
     * Finds the index whose name matches the given argument.
     *
     * @param name
     *     name of the object that is searched for in <code>this</code> schema.
     * @return
     *     the index that with the given name; {@code null} if not found.
     */
    public Index findIndex(String name)
    {
        return (Index) DatabaseObject.findByName(
                new ArrayList<DatabaseObject>(_baseConfiguration.getIndexes()), name);
    }
}
