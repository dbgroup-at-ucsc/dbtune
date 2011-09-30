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

import edu.ucsc.dbtune.util.Objects;

import java.sql.SQLException;
import java.util.Iterator;

/**
 * Abstraction of the highest level entry in the metadata hierarchy. A catalog is a container of 
 * Schema objects.
 *
 * @author Ivo Jimenez
 */
public class Catalog extends DatabaseObject implements Iterable<Schema>
{
    /**
     * Creates a new catalog with the given name
     *
     * @param name
     *     name of the catalog
     */
    public Catalog(String name)
    {
        super(name);
    }

    /**
     * Copy constructor
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
     *     fully qualified name of the new database object.
     * @return
     *     object corresponding to the given fully qualified name; {@code null} if not found.
     * @throws java.lang.ClassCastException
     *     if the {@link #findByQualifiedName} method returns an object whose type doesn't 
     *     correspond to the type given as parameter of the generic expression.
     * @see #findByQualifiedName
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
            container = findByQualifiedName(pathToObject.substring(0,pathToObject.lastIndexOf(".")));
        else
            container = this;

        if (container == null)
            throw new SQLException("Can't find parent of new object in " + pathToObject);

        pathElements = pathToObject.split("\\.");

        return container.newContainee(pathElements[pathElements.length-1]);
    }

    /**
     * Convenience method that casts the object returned by {@link #createObject}.
     *
     * @param pathToObject
     *     fully qualified name of the new database object.
     * @return
     *     object corresponding to the given fully qualified name; {@code null} if not found.
     * @throws java.lang.ClassCastException
     *     if the {@link #findByQualifiedName} method returns an object whose type doesn't 
     *     correspond to the type given as parameter of the generic expression.
     * @see #findByQualifiedName
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
     * Finds the schema whose name matches the given argument.
     *
     * @param name
     *     name of the index that is searched for in {@code this} catalog.
     * @return
     *     the index that has the given name; {@code null} if not found
     */
    public Index findIndex(String name) throws SQLException
    {
        if (name != null && name.contains("\\."))
            return this.<Index>findByName(name);

        DatabaseObject dbo = null;

        for(Schema s : this) {
            dbo = s.find(name);

            if(dbo != null)
                break;
        }

        if (dbo instanceof Index)
            return (Index) dbo;
            
        return null;
    }

    /**
     * returns the list of _schemas that the schema contains
     *
     * @return
     *     List of Schema objects
     */
    @Override
    public Iterator<Schema> iterator()
    {
        return Objects.<Iterator<Schema>>as(containees.iterator());
    }
    public Iterable<Schema> schemas()
    {
        return Objects.<Iterable<Schema>>as(containees);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseObject newContainee(String name) throws SQLException
    {
        return new Schema(this,name);
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
