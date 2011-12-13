package edu.ucsc.dbtune.metadata;

import java.util.ArrayList;
import java.util.List;

import java.sql.SQLException;

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
 * associated to it. In any case, an ID should be unique as is a fully qualified name.
 * <p>
 * With regards to containment, each containee has a pointer to its container. Each container is 
 * "notified" by its containee (in the containee's constructor) that it will now be a child of it. 
 * For an example of this take a look at the implementation of the {@link Table} constructor.
 *
 * @author Ivo Jimenez
 */
public abstract class DatabaseObject
{
    protected List<DatabaseObject> containees;
    protected DatabaseObject       container;

    protected String name;
    protected int    internalID;
    protected long   cardinality;
    protected long   pages;
    protected long   bytes;
    protected double creationCost;

    public static final int NON_ID = -1;
    
    /**
     * creates a dbobject with the given name constructor and contained on the given container
     *
     * @param container
     *    where to insert the object
     * @param name
     *    name of the db object
     */
    public DatabaseObject(DatabaseObject container, String name) throws SQLException
    {
        this.name         = name;
        this.internalID   = NON_ID;
        this.cardinality  = 0;
        this.pages        = 0;
        this.bytes        = 0;
        this.creationCost = 0.0;
        this.container    = null;
        this.containees   = new ArrayList<DatabaseObject>();
        this.container    = container;

        container.add(this);
    }

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
        this.container    = null;
        this.containees   = new ArrayList<DatabaseObject>();
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
        container    = dbo.container;
        containees   = dbo.containees;
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
    public void setPages(long pages)
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
     * Returns the object whose name matches the given fully qualified name. The top element is 
     * {@code this} object, that is, if {@code pathToObject="obj-name"} is sent and {@code 
     * contains("obj-name") == true}, then a reference to the object named {@code "obj-name"} is 
     * returned. In other words, the if an object named {@code "obj-name"} is contained in this 
     * object, the following is true {@code find("obj-name") == findByQualifiedName("obj-name")}).
     *
     * @param pathToObject
     *     fully qualified name of the new database object.
     * @return
     *     object corresponding to the given fully qualified name; {@code null} if not found.
     */
    protected final DatabaseObject findByQualifiedName(String pathToObject)
        throws SQLException
    {
        DatabaseObject dbo;
        String[]       pathElements;

        pathElements = pathToObject.split("\\.");

        if (pathElements.length == 0)
            throw new SQLException("Path string is empty");

        for (String element : pathElements)
            if (element.equals(""))
                throw new SQLException("All elements in path should contain at least one character");

        dbo = find(pathElements[0]);

        if (pathElements.length == 1)
            return dbo;
        else if (dbo == null)
            throw new SQLException("Can't find object " + pathElements[0] + "referenced in path");
        else
            return dbo.findByQualifiedName(pathToObject.substring(pathToObject.indexOf(".")+1,pathToObject.length()));
    }

    /**
     * Returns the container.
     *
     * @return
     *     container object
     */
    public DatabaseObject getContainer()
    {
        return container;
    }

    /**
     * Returns the object whose name matches the given one.
     *
     * @param name
     *     name of the object that is searched for in {@code this}
     * @return
     *     object corresponding to the given name; {@code null} if not found.
     */
    public DatabaseObject find(String name)
    {
        for (DatabaseObject containee : containees)
            if (containee.getName().equals(name)) return containee;

        return null;
    }

    /**
     * Returns the database object at the given ordinal position. For an object {@code dbo} the 
     * following should be true: {@code dbo.getOrdinalPosition() == 
     * dbo.getContainer().at(dbo.getOrdinalPosition())}
     *
     * @param position
     *     ordinal position of the retrieved object
     * @return
     *     the ordinal position of the object; {@code 0} if object isn't contained in other
     */
    public int getOrdinalPosition()
    {
        if (container == null)
            return 0;
        else
            return container.containees.indexOf(this) + 1;
    }

    /**
     * Returns the database object at the given zero-based ordinal position. For an object {@code 
     * dbo} the following should be true: {@code dbo.getOrdinalPosition() == 
     * dbo.getContainer().at(dbo.getOrdinalPosition()+1)}
     *
     * @param position
     *     ordinal position of the retrieved object
     * @return
     *     the reference to the object
     * @throws IndexOutOfBoundsException
     *     if the given position doesn't correspond to any of the contained objects
     */
    public DatabaseObject at(int position)
    {
        return containees.get(position);
    }

    /**
     * Finds the database object whose internal ID matches the given one.
     *
     * @param id
     *     id of the object that is searched for in {@code this}
     * @return
     *     the reference to the object; {@code null} if not found.
     */
    public DatabaseObject find(int id)
    {
        for (DatabaseObject containee : containees)
            if (containee.getInternalID() == id) return containee;

        return null;
    }

    /**
     * Removes an object.
     *
     * @param dbo
     *     dbo being removed
     */
    public void remove(DatabaseObject dbo) throws SQLException
    {
        containees.remove(dbo);
    }

    /**
     * Returns all the objects contained in the containment hierarchy. The order of the objects is 
     * not specified. This effectively flattens the containment tree.
     *
     * @param dbo
     *     dbo being removed
     */
    public List<DatabaseObject> getAll() throws SQLException
    {
        List<DatabaseObject> objects = new ArrayList<DatabaseObject>();

        objects.add(this);
        getAll(objects);

        return objects;
    }

    private final void getAll(List<DatabaseObject> objects) throws SQLException
    {
        for (DatabaseObject dbo : containees)
        {
            objects.add(dbo);
            dbo.getAll(objects);
        }
    }

    /**
     * Adds an element of this object
     *
     * @param dbo
     *     new object to add
     * @throws SQLException
     *     if object is already contained
     */
    final void add(DatabaseObject dbo) throws SQLException
    {
        if(containees.contains(dbo))
            throw new SQLException("Object " + dbo.getName() + " already in " + getName());

        if(!isValid(dbo))
            throw new SQLException("Object " + dbo.getName() + " with type " + dbo.getClass() + " not valid in " + getName());

        containees.add(dbo);
    }

    /**
     * returns the number of objects contained.
     *
     * @return
     *     number of objects contained
     */
    public final int size()
    {
        return containees.size();
    }

    /**
     * Returns the fully qualified name of the object objects contained in this one.
     *
     * @return
     *     list containing the contained database objects.
     */
    public final String getFullyQualifiedName()
    {
        if (container == null)
            return "";

        if (container.getFullyQualifiedName().equals(""))
            return getName();
        else
            return container.getFullyQualifiedName() + "." + getName();
    }

    /**
     * Returns the object whose name matches the given one.
     *
     * @param name
     *     name of the database object to search for.
     * @return
     *     {@code true} if contained; {@code false} otherwise.
     */
    public final boolean contains(String name)
    {
        return find(name) != null;
    }

    /**
     * Checks if the type of the given database object is valid. With valid we mean that it can be 
     * contained by {@code this}.
     *
     * @param dbObject
     *     database object whose type is checked to see if an instance of it could be added to 
     *     {@code this} one.
     */
    abstract boolean isValid(DatabaseObject dbObject);

    /**
     * Creates and adds a database object with the given name.
     *
     * @param name
     *     name of the new database object.
     * @return
     *     the constructed object
     * @throws SQLException
     *     if an object with the given name is already contained in this object.
     * @see #addObject
     */
    abstract DatabaseObject newContainee(String name)
        throws SQLException;

    /**
     * Compares another object against this one and determines if they're equal or not. Note that 
     * this is different from {@link #equals}.
     *
     * @return
     *     {@code true} if contents of given object are equal to this; {@code false} otherwise.
     */
    public abstract boolean equalsContent(Object other);

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof DatabaseObject))
            return false;
        DatabaseObject dbo = (DatabaseObject) other;

        return getFullyQualifiedName().equals(dbo.getFullyQualifiedName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return getFullyQualifiedName().hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return getFullyQualifiedName();
    }
}
