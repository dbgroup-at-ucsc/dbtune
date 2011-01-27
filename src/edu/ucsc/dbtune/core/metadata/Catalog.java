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
 * Abstraction of the dictionary used to save metadata. A catalog can be viewed
 * as a container of Schema objects.
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class Catalog extends DatabaseObject
{

    private List<Schema> schemas;

    /**
     * default constructor
     */
    public Catalog(){
        schemas = new ArrayList<Schema>();
    }

    /**
     * Creates a new catalog with the given name
     *
     * @param name
     *     name of the catalog
     */
    public Catalog(String name)
    {
        this.name = name;
        schemas = new ArrayList<Schema>();
    }

    /**
     * adds a schema to a catalog
     *
     * @param schema
     *     new table to add
     */
    public void add(Schema schema)
    {
        schemas.add(schema);
        schema.setCatalog(this);
    }

    /**
     * returns the list of schemas that the schema contains
     *
     * @return
     *     List of Schema objects
     */
    public List<Schema> getSchemas()
    {
        return new ArrayList<Schema>(schemas);
    }
}
