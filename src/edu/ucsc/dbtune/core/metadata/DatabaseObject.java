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

/**
 * The abstraction of a database object. A database object is any data structure that stores 
 * information about objects contained in a database such as Tables, Views, Columns, etc.
 * <p>
 * Each containee should have a pointer to its container; each containee is "notified" (eg. a Column 
 * has method setTable() ) by its container. This means that the containee doesn't "notify" its 
 * container that will contain it, rather the container "notifies" the containee.
 * <p>
 * For an example of the above: see the {@link Table#add} method
 *
 * @author Ivo Jimenez
 * @see Table#add
 */
public abstract class DatabaseObject
{
    protected String name;
    protected int    id; // -1 means UNASSIGNED id
    protected long   cardinality;
    protected long   pages;
    protected long   size;
    protected double creationCost;

    /**
     * default constructor
     */
    public DatabaseObject(int  ID)
    {
        name         = null;
        id           = ID;
        cardinality  = 0;
        pages        = 0;
        size         = 0;
        creationCost = 0.0;
    }

    /**
     * creates a dbobject with the given name constructor
     *
     * @param name
     *    name of the db object
     */
    public DatabaseObject( String name )
    {
        this(-1);

        this.name = name;
    }

    /**
     * copy constructor
     *
     * @param dbo
     *    other database object to be copied in the new one
     */
    public DatabaseObject( DatabaseObject dbo )
    {
        this(dbo.id);

        name         = dbo.name;
        cardinality  = dbo.cardinality;
        pages        = dbo.pages;
        size         = dbo.size;
        creationCost = dbo.creationCost;
    }

    /**
     * Assigns the name of the database object.
     *
     * @param name value representing the object's name
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * Returns the name of the object.
     *
     * @return
     *     string value representing the object's name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Returns the cost of creating the object
     *
     * @return
     *     cost of materializing the object
     */
    public void setCreationCost(double cost)
    {
        creationCost = cost;
    }

    /**
     * Returns the cost of creating the index
     *
     * @return
     *     cost of materializing the index
     */
    public double getCreationCost()
    {
        return creationCost;
    }

    /**
     * Assigns the id of the database object.
     *
     * @param id id of the database object
     */
    public void setId(int id)
    {
        this.id = id;
    }

    /**
     * Returns the id of the object.
     *
     * @return
     *     long value representing the object's id; -1 if it hasn't been assigned
     */
    public int getId()
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
     * @return
     *     cardinality value
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
     * @return
     *     number of pages
     */
    public long getPages()
    {
        return pages;
    }

    /**
     * Returns the number of bytes that the object occupies in disk
     *
     * @return
     *     size in megabytes
     */
    public long getMegaBytes()
    {
        return size;
    }

    /**
     * Evaluates equality based on the internal id of the object
     */
    @Override
    public boolean equals(Object other)
    {
        return (other instanceof DatabaseObject) && (((DatabaseObject)other).id == id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return (new Long(id)).hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        if (name == null) {
            return new String("");
        } else {
            return new String(name);
        }
    }

    /**
     * Finds a database object that is contained in the given list whose name matches the given 
     * method argument.
     *
     * @param name
     *     name of the object that is searched for in <code>this</code>
     * @return
     *     the reference to the object; null if not found
     */
    protected static DatabaseObject findByName( List<DatabaseObject> objects, String name )
    {
        for (DatabaseObject containee : objects)
        {
            if (containee.getName().equals(name))
            {
                return containee;
            }
        }

        return null;
    }
}
