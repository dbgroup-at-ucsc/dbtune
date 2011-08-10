/*
 ******************************************************************************
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
 ******************************************************************************/
package edu.ucsc.dbtune.metadata;

import java.util.List;
import java.util.ArrayList;

/**
 * A configuration represents a set of physical structures over tables (and columns) that are used 
 * to improve the performance of DML statements in a database. A configuration is typically composed 
 * of a set of indexes, but can also contain materialized views (MV), partitions, denormalizations 
 * and many others.
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class Configuration extends DatabaseObject
{
    protected List<Index> _indexes;

    /**
     * default constructor
     */
    public Configuration(List<Index> indexes)
    {
        super(-1);
        this._indexes  = new ArrayList<Index>(indexes);
    }

    /**
     * copy constructor
     *
     * @param other
     *     other configuration being copied
     */
    public Configuration(Configuration other)
    {
        super(other);

        _indexes = other._indexes;
    }

    /**
     * constructs a new configuration with given name
     *
     * @param name
     *     name of the configuration
     */
    public Configuration(String name)
    {
        super(name);
        this._indexes = new ArrayList<Index>();
    }

    /**
     * adds an index to the schema
     *
     * @param index
     *     new index to add
     */
    public void add(Index index)
    {
        _indexes.add(index);
    }

    /**
     * returns the list of indexes that the schema contains
     *
     * @return
     *     List of Index objects
     */
    public List<Index> getIndexes()
    {
        return new ArrayList<Index>(_indexes);
    }
}
