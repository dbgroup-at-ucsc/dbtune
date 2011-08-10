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
 * Abstraction of the dictionary used to save metadata. A catalog can be viewed
 * as a container of Schema objects.
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class Catalog extends DatabaseObject
{
    protected List<Schema> _schemas;

    /**
     * default constructor
     */
    public Catalog()
    {
        super(-1);
        _schemas = new ArrayList<Schema>();
    }

    /**
     * copy constructor
     *
     * @param catalog
     *     other catalog copied into a new one
     */
    public Catalog( Catalog catalog )
    {
        super(catalog);

        _schemas = catalog._schemas;
    }

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
     */
    public void add(Schema schema)
    {
        _schemas.add(schema);
        schema.setCatalog(this);
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
        return (Schema) DatabaseObject.findByName(new ArrayList<DatabaseObject>(_schemas),name);
    }

}
