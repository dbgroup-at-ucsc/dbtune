package edu.ucsc.dbtune.metadata;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A configuration represents a set of physical structures over tables (and/or columns) that are 
 * used to improve the performance of DML statements in a database. A configuration is typically 
 * composed of a set of indexes, but can also contain materialized views (MV), partitions, 
 * denormalizations and many others.
 *
 * @author Ivo Jimenez
 */
public class Configuration implements Iterable<Index>
{
    protected List<Index> _indexes;
    protected String      _name;

    /**
     * constructs an empty configuration with given name.
     *
     * @param name
     *     name of the configuration
     */
    public Configuration(String name)
    {
        this(name,new ArrayList<Index>());
    }

    /**
     * constructs a new configuration with the given iterable set of indexes.
     *
     * @param indexes
     *     an iterable set of indexes that comprise the configuration
     */
    public Configuration(Iterable<Index> indexes)
    {
        this("", new ArrayList<Index>());

        for (Index idx : indexes)
            add(idx);
    }

    /**
     * constructs a new configuration with the given list of indexes.
     *
     * @param indexes
     *     list of indexes that comprise the configuration
     */
    public Configuration(List<Index> indexes)
    {
        this("",indexes);
    }

    /**
     * constructs a new configuration with given name and list of indexes.
     *
     * @param name
     *     name of the configuration
     * @param indexes
     *     list of indexes that comprise the configuration
     */
    public Configuration(String name, List<Index> indexes)
    {
        this._name    = name;
        this._indexes = new ArrayList<Index>(indexes);
    }

    /**
     * copy constructor
     *
     * @param other
     *     other configuration being copied
     */
    public Configuration(Configuration other)
    {
        this(other._name, other._indexes);
    }

    /**
     * adds an index to the schema. If the index is already contained it's not added.
     *
     * @param index
     *     new index to add
     */
    public void add(Index index)
    {
        if(!_indexes.contains(index)) {
            _indexes.add(index);
        }
    }

    /**
     * returns the list of indexes that the schema contains
     *
     * @return
     *     List of Index objects
     */
    public List<Index> toList()
    {
        return new ArrayList<Index>(_indexes);
    }

    /**
     * Returns the number of elements in the configuration
     *
     * @return
     *     number of elements
     */
    public int size()
    {
        return _indexes.size();
    }

    /**
     * Whether or not this configuration contains any elements.
     *
     * @return
     *     {@code true} if empty; {@code false} otherwise.
     */
    public boolean isEmpty()
    {
        return _indexes.isEmpty();
    }

    /**
     * checks if a given index is contained in the configuration.
     *
     * @param index
     *     index that is looked for as part of this configuration
     */
    public boolean contains(Index index)
    {
        return _indexes.contains(index);
    }

    /**
     * checks if a given index is contained in the configuration. The comparison is done using the 
     * {@link DatabaseObject#equalsContent} method.
     *
     * @param index
     *     index that is looked for as part of this configuration
     */
    public boolean containsContent(Index index)
    {
        for (Index idx : _indexes)
            if (idx.equalsContent(index))
                return true;

        return false;
    }

    /**
     * Whether the given configuration is contained in this one.
     *
     * @return
     *     {@code true} if all elements of given configuration are contained in this configuration; 
     *     {@code false} otherwise.
     */
    public boolean contains(Configuration configuration)
    {
        for(Index idx : configuration) {
            if(!contains(idx)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the position of the index with respect to the set of indexes contained in the 
     * configuration. The ordering is completely arbitrary and determined by the order in which the 
     * {@link #add} method is invoked or by the order in which indexes are contained in the list 
     * given to the constructor.
     *
     * @param index
     *     index whose position is retrieved
     */
    public int getOrdinalPosition(Index index)
    {
        return _indexes.indexOf(index);
    }

    /**
     * Returns the index that has the given ordinal position.
     *
     * @param position
     *     ordinal position of the index that is looked for
     * @return
     *     reference to index if found
     * @throws ArrayIndexOutOfBoundsException
     *     if the index is out of bounds with respect to the number of indexes contained in the 
     *     configuration
     * @see #getOrdinalPosition
     */
    public Index getIndexAt(int position)
    {
        return _indexes.get(position);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Configuration))
            return false;

        Configuration conf = (Configuration) other;

        if (_indexes.containsAll(conf._indexes) && conf._indexes.containsAll(_indexes))
            return true;

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Index> iterator()
    {
        return _indexes.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder str = new StringBuilder();
        
        for(Index idx : this) {
            str.append("    " + idx + "\n");
        }

        return str.toString();
    }
}
