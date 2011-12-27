package edu.ucsc.dbtune.util;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * This class implements the {@link Set} interface, backed by a bit array (actually a {@link BitSet} 
 * instance). This class offers constant time performance for the basic operations (add, remove, 
 * contains and size).
 * <p>
 * Iterating over this set requires time proportional to the sum of the number of set bits.
 * <p>
 * This class could potentially implement the {@link SortedSet} interface instead.
 *
 * @param <E>
 *      an implementation of the {@link Identifiable} class.
 * @author Karl Schnaitter
 * @author Ivo Jimenez
 */
public class BitArraySet<E extends Identifiable> extends AbstractSet<E> implements Set<E>
{
    private static final BitSet t = new BitSet();
    private BitSet bitSet;
    private Map<Integer, E> elements;
    
    /**
     * Constructs an empty configuration.
     */
    public BitArraySet()
    {
        bitSet = new BitSet();
        elements = new HashMap<Integer, E>(100);
    }

    /**
     * Constructs a copy of the given configuration.
     *
     * @param ibs
     *      the bitSet being copied
     */
    public BitArraySet(BitArraySet<E> ibs)
    {
        bitSet = (BitSet) ibs.bitSet.clone();
        elements = new HashMap<Integer, E>(ibs.elements);
    }

    /**
     * Constructs a set containing the elements in the given collection.
     *
     * @param other
     *      a collection of objects
     */
    public BitArraySet(Collection<E> other)
    {
        this();

        for (E e : other)
            add(e);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean add(E e)
    {
        boolean wasContained = bitSet.get(e.getId());
        bitSet.set(e.getId());
        elements.put(e.getId(), e);
        return !wasContained;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean addAll(Collection<? extends E> c)
    {
        if (c instanceof BitArraySet<?>) {
            BitSet added = (BitSet) ((BitArraySet<?>) c).bitSet.clone();
            BitSet both = (BitSet) ((BitArraySet<?>) c).bitSet.clone();

            both.and(bitSet);
            bitSet.or(added);

            added.xor(both);

            BitArraySet<?> bas = (BitArraySet<?>) c;

            for (int i = added.nextSetBit(0); i >= 0; i = added.nextSetBit(i + 1)) {
                elements.put(new Integer(i), (E) bas.elements.get(i));
            }

            return !added.isEmpty();

        } else {
            boolean modified = false;

            for (E e : c)
                if (add(e))
                    modified = true;

            return modified;
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void clear()
    {
        bitSet.clear();
        elements.clear();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(Object o)
    {
        if (!(o instanceof Identifiable))
            return false;

        return bitSet.get(((Identifiable) o).getId());
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean containsAll(Collection<?> c)
    {
        if (c instanceof BitArraySet<?>) {
            BitArraySet<?> o = (BitArraySet<?>) c;

            synchronized (t) {
                t.clear();
                t.or(o.bitSet);
                t.and(bitSet);
                return t.equals(o.bitSet);
            }
        } else
            for (Object i : c)
                if (!(i instanceof Identifiable))
                    return false;
                else if (!bitSet.get(((Identifiable) i).getId()))
                    return false;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return bitSet.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty()
    {
        return bitSet.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    public Iterator<E> iterator()
    {
        return new BitArraySetIterator(bitSet, elements.values().iterator());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(Object o)
    {
        if (!(o instanceof Identifiable))
            return false;

        BitSet beforeBS = (BitSet) bitSet.clone();

        bitSet.clear(((Identifiable) o).getId());

        elements.remove(((Identifiable) o).getId());

        return !beforeBS.equals(bitSet);
    }

    // NOTE: removeAll could use andNot if the AbstractSet.removeAll implementation sucks
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean retainAll(Collection<?> c)
    {

        if (c instanceof BitArraySet<?>) {
            BitSet before = (BitSet) bitSet.clone();

            bitSet.and(((BitArraySet<?>) c).bitSet);

            BitSet removed = (BitSet) bitSet.clone();

            removed.xor(before);

            for (int i = removed.nextSetBit(0); i >= 0; i = removed.nextSetBit(i + 1))
                elements.remove(i);

            return !removed.isEmpty();
        } else {
            boolean modified = false;
            ArrayList<Identifiable> container = new ArrayList<Identifiable>(elements.values());

            for (Identifiable o : container)
                if (!c.contains(o)) {
                    remove(o);
                    modified = true;
                }

            return modified;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;

        Collection<?> c = (Collection<?>) o;

        if (c.size() != size())
            return false;

        try {
            return containsAll(c);
        }
        catch (ClassCastException unused) {
            return false;
        }
        catch (NullPointerException unused) {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size()
    {
        return elements.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object[] toArray()
    {
        return elements.values().toArray();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T[] toArray(T[] array)
    {
        return elements.values().toArray(array);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return bitSet.toString();
    }

    /**
     * An iterator of the bit array that provides a {@link Iterator#remove} implementation.
     *
     * @author Ivo Jimenez
     */
    private class BitArraySetIterator implements Iterator<E>
    {
        private BitSet bitSet;
        private Iterator<E> delegate;
        private E current;

        /**
         * @param bitSet
         *      the bitset used to update when a {@code remove()} is invoked
         * @param delegate
         *      an iterator used to delegate the other {@code next()} and {@code hasNext()} 
         *      operations
         */
        public BitArraySetIterator(BitSet bitSet, Iterator<E> delegate)
        {
            this.bitSet = bitSet;
            this.delegate = delegate;
            this.current = null;
        }

        /**
         * {@inheritDoc}
         */
        public E next()
        {
            current = delegate.next();
            return current;
        }

        /**
         * {@inheritDoc}
         */
        public void remove()
        {
            delegate.remove();
            bitSet.clear(current.getId());
        }

        /**
         * {@inheritDoc}
         */
        public boolean hasNext()
        {
            return delegate.hasNext();
        }
    }
}
