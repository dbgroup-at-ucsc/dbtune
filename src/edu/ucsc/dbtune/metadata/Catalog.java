package edu.ucsc.dbtune.metadata;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.ucsc.dbtune.util.Objects;

/**
 * Abstraction of the highest level entry in the metadata hierarchy. A catalog is a container of 
 * Schema objects.
 *
 * @author Ivo Jimenez
 */
public class Catalog extends DatabaseObject implements Iterable<Schema>
{
    /**
     * Creates a new catalog with the given name.
     *
     * @param name
     *     name of the catalog
     */
    public Catalog(String name)
    {
        super(name);
    }

    /**
     * Copy constructor.
     *
     * @param other
     *     other catalog object
     */
    public Catalog(Catalog other)
    {
        super(other);
    }

    /**
     * Convenience method that casts the object returned by {@link #findByQualifiedName}.
     *
     * @param pathToObject
     *      fully qualified name of the new database object.
     * @return
     *      object corresponding to the given fully qualified name; {@code null} if not found.
     * @throws SQLException
     *      for the same reasons that {@link #findByQualifiedName} throws one
     * @throws java.lang.ClassCastException
     *      if the {@link #findByQualifiedName} method returns an object whose type doesn't 
     *      correspond to the type given as parameter of the generic expression.
     * @see #findByQualifiedName
     * @param <T>
     *      the object that is expected to be retrieved, i.e. the object type corresponding to the 
     *      given fully qualified name
     */
    public final <T extends DatabaseObject> T findByName(String pathToObject)
        throws SQLException
    {
        return Objects.<T>as(findByQualifiedName(pathToObject));
    }

    /**
     * Creates a database object identified by the given fully qualified name. Only leaf objects are 
     * created. For example, if {@code 'schema_one.table_1.column_2'} is given and {@code 'table_1'} 
     * doesn't exist, an exception will be thrown.
     * <p>
     * Client code is in charge of casting appropriately. The type of the object created is 
     * determined by the implementation of the {@link #newContainee} method.
     *
     * @param pathToObject
     *     fully qualified name of the new database object.
     * @return
     *     the new database object
     * @throws SQLException
     *     if the path (up to the object's parent) doesn't exist; if the container already has an 
     *     element with that name contained in it
     * @see #findByQualifiedName
     * @see #newContainee
     */
    public final DatabaseObject createObject(String pathToObject) throws SQLException
    {
        DatabaseObject container;
        String[]       pathElements;
        
        if (pathToObject.contains("."))
            container =
                findByQualifiedName(pathToObject.substring(0, pathToObject.lastIndexOf(".")));
        else
            container = this;

        if (container == null)
            throw new SQLException("Can't find parent of new object in " + pathToObject);

        pathElements = pathToObject.split("\\.");

        return container.newContainee(pathElements[pathElements.length - 1]);
    }

    /**
     * Convenience method that casts the object returned by {@link #createObject}.
     *
     * @param pathToObject
     *      fully qualified name of the new database object.
     * @return
     *      object corresponding to the given fully qualified name; {@code null} if not found.
     * @throws java.lang.ClassCastException
     *      if the {@link #findByQualifiedName} method returns an object whose type doesn't 
     *      correspond to the type given as parameter of the generic expression.
     * @see #findByQualifiedName
     * @param <T>
     *      the object that is expected to be retrieved, i.e. the object type corresponding to the 
     *      given fully qualified name
     * @throws SQLException
     *      when {@link #createObject} throws an exception
     * @see #createObject
     */
    public final <T> T create(String pathToObject) throws SQLException
    {
        return Objects.<T>as(createObject(pathToObject));
    }

    /**
     * Finds the schema whose name matches the given argument. Convenience method that accomplishes 
     * what {@link #find} does but without requiring the user to cast. In other words, the following 
     * is true {@code findSchema("name") == (Schema)find("name")}.
     *
     * @param name
     *     name of the schema that is searched for in {@code this} catalog.
     * @return
     *     the schema that has the given name; {@code null} if not found
     */
    public Schema findSchema(String name)
    {
        return (Schema) find(name);
    }

    /**
     * Finds the index whose name matches the given argument, across all the schemas contained in 
     * the catalog. If more than one index with the same name exists, this method makes no 
     * guarantees on which gets returned, that is, this method returns the "first" to be found, 
     * where "first" is ambiguous.
     *
     * @param name
     *      name of the index that is searched for in {@code this} catalog.
     * @return
     *      the index that has the given name; {@code null} if not found
     * @throws SQLException
     *      when {@code name} is a fully qualified path, and there's an error searching by it
     * @see #findByQualifiedName
     */
    public Index findIndex(String name) throws SQLException
    {
        if (name != null && name.contains("\\."))
            return this.<Index>findByName(name);

        DatabaseObject dbo = null;

        for (Schema s : this) {
            dbo = s.find(name);

            if (dbo != null)
                break;
        }

        if (dbo instanceof Index)
            return (Index) dbo;
            
        return null;
    }

    /**
     * returns the list of schemas that the catalog contains.
     *
     * @return
     *     list of Schema objects
     */
    @Override
    public Iterator<Schema> iterator()
    {
        return Objects.<Iterator<Schema>>as(containees.iterator());
    }

    /**
     * returns the list of schemas that the catalog contains.
     *
     * @return
     *      the schemas contained in the catalog
     */
    public List<Schema> schemas()
    {
        return new ArrayList<Schema>(Objects.<List<Schema>>as(containees));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseObject newContainee(String name) throws SQLException
    {
        return new Schema(this, name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(DatabaseObject dbo)
    {
        return dbo instanceof Schema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equalsContent(Object other)
    {
        throw new RuntimeException("not implemented yet");
    }
}
