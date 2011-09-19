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

import java.util.List;

/**
 * The abstraction of a database object. A database object is any data structure that stores 
 * information about objects contained in a database such as Tables, Views, Columns, etc.
 * <p>
 * An object is identified by its fully qualified name. For example, the identification of a column 
 * {@code employee_id} contained in a {@code employee} table of the {@code company} schema of a 
 * {@code enterpriseX} database would be {@code enterpriseX.company.employee.employee_id}, i.e. its 
 * fully qualified name all the way up to the catalog: {@code catalog.schema.table.column}.
 * <p>
 * Besides this fully qualified identification, a specific DBMS may have additional ways of 
 * internally identifying objects. For example, in Postgres a table also has a {@code oid} 
 * associated to it.
 * <p>
 * With regards to containment, each containee should have a pointer to its container. Each 
 * container is "notified" by its containee (in the containee's constructor) that it will now be a 
 * child of it. For an example of this take a look at the implementation of the {@link Table} 
 * constructor.
 *
 * @author Ivo Jimenez
 * @see Table#add
 */
public abstract class DatabaseObject
{
    protected String name;
    protected int    internalID;
    protected long   cardinality;
    protected long   pages;
    protected long   bytes;
    protected double creationCost;

    public static final int NON_ID = -1;
    
    /**
     * creates a dbobject with the given name constructor
     *
     * @param name
     *    name of the db object
     */
    public DatabaseObject(String name)
    {
        this.name         = name;
        this.internalID   = NON_ID;
        this.cardinality  = 0;
        this.pages        = 0;
        this.bytes        = 0;
        this.creationCost = 0.0;
    }

    /**
     * copy constructor
     *
     * @param dbo
     *    other database object to be copied in the new one
     */
    public DatabaseObject(DatabaseObject dbo)
    {
        name         = dbo.name;
        cardinality  = dbo.cardinality;
        internalID   = dbo.internalID;
        pages        = dbo.pages;
        bytes        = dbo.bytes;
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
     * @param cost
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
     * Assigns the internal ID of the object, which is used to identify the object inside the DBMS. 
     * For some {@link DatabaseObject} implementations, this isn't used internally by a DBMS. Also,   
     * in some DB systems, this isn't used at all.
     *
     * @param id id of the database object
     */
    public void setInternalID(int id)
    {
        internalID = id;
    }

    /**
     * Returns the internal ID of the object, which is used to identify the object inside the DBMS. 
     * For some {@link DatabaseObject} implementations, this isn't used internally by a DBMS. Also,   
     * in some DB systems, this isn't used at all.
     *
     * @return
     *     value representing the object's internal id; {@link #NON_ID} if it hasn't been assigned
     */
    public int getInternalID()
    {
        return internalID;
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
     * Assigns the number of bytes that the object occupies in disk
     *
     * @param bytes
     *     size in bytes
     */
    public void setBytes(long bytes)
    {
        this.bytes = bytes;
    }

    /**
     * Returns the number of bytes that the object occupies in disk
     *
     * @return
     *     size in bytes
     */
    public long getBytes()
    {
        return bytes;
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
     * {@inheritDoc}
     */
    @Override
    public abstract boolean equals(Object other);

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract int hashCode();

    /**
     * Returns the string representation of the SQL statement that creates the hypothetical 
     * structure inside the DBMS that represents this object.
     *
     * @return
     *     the SQL statement
    public abstract String getCreateHypotheticalStatement();
    public abstract void getCreateHypotheticalStatement();
    public abstract void getMaterializeCreateStatement();
    public abstract void getHypotheticalDropStatement();
    public abstract void getMaterializeDropStatement();
    */

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
            if (containee.getName().equals(name))
                return containee;

        return null;
    }

    /**
     * Finds a database object that is contained in the given list whose internal ID matches the 
     * given method argument.
     *
     * @param id
     *     id of the object that is searched for in <code>this</code>
     * @return
     *     the reference to the object; {@code null} if not found.
     */
    protected static DatabaseObject findByInternalID(List<DatabaseObject> objects, int id)
    {
        for (DatabaseObject containee : objects)
            if (containee.getInternalID() == id)
                return containee;

        return null;
    }
}
