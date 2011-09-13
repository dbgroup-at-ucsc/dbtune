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
 * A schema is a container of database objects, regularly tables, indexes, views, etcetera.
 *
 * @author Ivo Jimenez
 */
public class Schema extends DatabaseObject
{
    protected List<Table> _tables;

    protected Catalog       _catalog;
    protected Configuration _baseConfiguration;

    /**
     * constructs a new schema whose name is given
     *
     * @param name
     *     name of the schema
     * @param catalog
     *     catalog where the new schema will be contained
     * @throws SQLException
     *     if a schema with the given name is already contained in the catalog
     */
    public Schema(Catalog catalog, String name) throws SQLException
    {
        super(name);

        _tables  = new ArrayList<Table>();
        _catalog = catalog;

        _catalog.add(this);
    }

    /**
     * copy constructor
     *
     * @param other
     *     other schema being copied
     */
    protected Schema(Schema other)
    {
        super(other);

        _tables = other._tables;
        _baseConfiguration = other._baseConfiguration;
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
     * @throws SQLException
     *     if table is already contained in the schema
     */
    void add(Table table) throws SQLException
    {
        if(_tables.contains(table))
            throw new SQLException("Table " + table + " already in table");

        _tables.add(table);
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
     * Finds the table whose id matches the given argument.
     *
     * @param id
     *     id of the object that is searched for in <code>this</code> schema.
     * @return
     *     the table that has the given name; {@code null} if not found
     */
    public Table findTable(int id)
    {
        return (Table) DatabaseObject.findByInternalID(new ArrayList<DatabaseObject>(_tables),id);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof Schema))
            return false;

        Schema sch = (Schema) other;

        return _catalog == sch._catalog && name.equals(sch.name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return 31 * _catalog.hashCode() * name.hashCode();
    }
}
