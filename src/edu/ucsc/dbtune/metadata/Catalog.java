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
 * Abstraction of the highest level entry in the metadata hierarchy. A catalog is a container of 
 * Schema objects.
 *
 * @author Ivo Jimenez
 */
public class Catalog extends DatabaseObject
{
    protected List<Schema> _schemas;

    /**
     * Creates a new catalog with the given name
     *
     * @param name
     *     name of the catalog
     */
    public Catalog(String name)
    {
        super(name);
        _schemas = new ArrayList<Schema>();
    }

    /**
     * adds a schema to a catalog
     *
     * @param schema
     *     new table to add
     * @throws SQLException
     *     if schema is already contained in the catalog
     */
    void add(Schema schema) throws SQLException
    {
        if(_schemas.contains(schema))
            throw new SQLException("Schema " + schema + " already in catalog");

        _schemas.add(schema);
    }

    /**
     * returns the list of _schemas that the schema contains
     *
     * @return
     *     List of Schema objects
     */
    public List<Schema> getSchemas()
    {
        return new ArrayList<Schema>(_schemas);
    }

    /**
     * Finds the schema whose name matches the given argument.
     *
     * @param name
     *     name of the object that is searched for in <code>this</code> catalog.
     * @return
     *     the schema that has the given name; {@code null} if not found
     */
    public Schema findSchema(String name)
    {
        return (Schema) findByName(new ArrayList<DatabaseObject>(_schemas),name);
    }

    /**
     * Finds the index whose name matches the given argument.
     *
     * @param name
     *     name of the index that is searched for in <code>this</code> catalog.
     * @return
     *     the schema that has the given name; {@code null} if not found
     */
    public Index findIndex(String name)
    {
        Index idx = null;

        for(Schema s : _schemas) {
            idx = (Index) findByName(new ArrayList<DatabaseObject>(s.getIndexes()),name);

            if(idx != null)
                break;
        }
            
        return idx;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof Catalog))
            return false;

        Catalog cat = (Catalog) other;

        return name.equals(cat.name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return name.hashCode();
    }
}
