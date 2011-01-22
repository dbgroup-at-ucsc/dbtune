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
 * The abstraction of a database object. A database object is any data structure that stores 
 * information about objects contained in a database such as Tables, Views, Columns, etc.
 * <p>
 * // TODO describe in more detail the hierarchy containment policy
 * Each containee should have a pointer to its container; each containee is "notified" (eg. a Column 
 * has method setTable() ) by its container. This means that the containee doesn't "notify" its 
 * container that will contain it, rather the container "notifies" the containee.
 * <p>
 * Example of the above: see Table.add(Column)
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public abstract class DatabaseObject
{
    protected String name;
    protected long   id;
    protected long   cardinality;
    protected long   pages;
    protected long   size;

    /**
     * Returns the name of the object.
     *
     * @return string value representing the object's name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Assigns the id of the database object.
     *
     * @param id id of the database object
     */
    public void setId(long id)
    {
        this.id = id;
    }

    /**
     * Returns the id of the object.
     *
     * @return long value representing the object's id
     */
    public long getId()
    {
        return id;
    }

    /**
     * Assigns the cardinality of the object. This has different meanings depending on whose 
     * extending the class.
     *
     * @param cardinality
     */
    public void setCardinality( long cardinality )
    {
        this.cardinality = cardinality;
    }

    /**
     * Returns the cardinality of the object
     *
     * @return cardinality value
     */
    public long getCardinality()
    {
        return cardinality;
    }

    /**
     * Assigns the number of pages that the object occupies in disk. 
     *
     * @param pages
     */
    public void setPages( long pages )
    {
        this.pages = pages;
    }

    /**
     * Returns the number of pages that the object occupies in disk
     *
     * @return number of pages
     */
    public long getPages()
    {
        return pages;
    }

    /**
     * returns the string representation of the object
     *
     * @return String value of the database object
     */
    public String toString()
    {
        return name;
    }
}
