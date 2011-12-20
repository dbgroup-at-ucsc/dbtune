package edu.ucsc.dbtune.metadata;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.util.Objects;

/**
 * A schema is a container of database objects, regularly tables, indexes, views, etcetera.
 *
 * @author Ivo Jimenez
 */
public class Schema extends DatabaseObject
{
    /**
     * constructs a new schema whose name is given
     *
     * @param name
     *     name of the schema
     * @param catalog
     *     catalog where the new schema will be contained
     * @throws SQLException
     *     if a schema with the given name is already contained in the catalog
     */
    public Schema(Catalog catalog, String name) throws SQLException
    {
        super(catalog, name);
    }

    /**
     * copy constructor
     *
     * @param other
     *     other schema being copied
     */
    protected Schema(Schema other)
    {
        super(other);
    }

    /**
     * Returns the schema's base configuration
     *
     * @return
     *     configuration corresponding to the schema
     */
    public Set<Index> getBaseConfiguration()
    {
        List<Index> indexes = new ArrayList<Index>();

        for (DatabaseObject dbo : containees)
        {
            Index index;

            if (dbo instanceof Index)
                index = (Index) dbo;
            else
                continue;

            if (index.isMaterialized())
                indexes.add(index);
        }

        return new HashSet<Index>(indexes);
    }

    /**
     * Returns the catalog that contains the schema. Convenience method that accomplishes what 
     * {@link #getContainer} does but without requiring the user to cast. In other words, the 
     * following is true {@code getCatalog() == (Catalog)getContainer()}.
     *
     * @return
     *     the catalog that contains this object
     */
    public Catalog getCatalog()
    {
        return (Catalog) container;
    }

    /**
     * Finds the table whose name matches the given argument. Convenience method that accomplishes 
     * what {@link #find} does but without requiring the user to cast. In other words, the following 
     * is true {@code findTable("name") == (Table)find("name")}.
     *
     * @param name
     *     name of the object that is searched for in <code>this</code> schema.
     * @return
     *     the table that has the given name; {@code null} if not found
     */
    public Table findTable(String name)
    {
        return (Table) find(name);
    }

    /**
     * Finds the table whose id matches the given argument.
     *
     * @param id
     *     id of the object that is searched for in <code>this</code> schema.
     * @return
     *     the table that has the given name; {@code null} if not found
     */
    public Table findTable(int id)
    {
        return (Table) find(id);
    }

    /**
     * Finds the index whose name matches the given argument. Convenience method that accomplishes 
     * what {@link #find} does but without requiring the user to cast. In other words, the following 
     * is true {@code findIndex("name") == (Index)find("name")}.
     *
     * @param name
     *     name of the object that is searched for in <code>this</code> schema.
     * @return
     *     the index that with the given name; {@code null} if not found.
     */
    public Index findIndex(String name)
    {
        return (Index) find(name);
    }

    /**
     * Returns an iterator of indexes.
     *
     * @return
     *     an iterator over the indexes defined for the schema
     */
    public List<Index> indexes()
    {
        List<Index> list = new ArrayList<Index>();

        for (DatabaseObject dbo : containees)
        {
            Index index;

            if (dbo instanceof Index)
                index = (Index) dbo;
            else
                continue;

            list.add(index);
        }

        return list;
    }

    /**
     * Returns an iterator of indexes.
     *
     * @return
     *     an iterator over the indexes defined for the schema
     */
    public Iterable<Table> tables()
    {
        List<Table> list = new ArrayList<Table>();

        for (DatabaseObject dbo : containees)
        {
            Table table;

            if (dbo instanceof Table)
                table = (Table) dbo;
            else
                continue;

            list.add(table);
        }

        return list;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseObject newContainee(String name) throws SQLException
    {
        return new Index(this,name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(DatabaseObject containee)
    {
        return containee instanceof Table || containee instanceof Index;
        // XXX: determine whether or not the properties of an added index have to be checked. For
        //      instance, if an index is already contained and is CLUSTERED, no other index can be
        //      added that is also CLUSTERED. Similarly for PRIMARY/SECONDARY.
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
