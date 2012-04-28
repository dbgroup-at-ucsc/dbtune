package edu.ucsc.dbtune.util;

import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

/**
 * @param <E>
 *      the class of objects to place in the queue
 * @author Karl Schnaitter
 */
public class DefaultQueue<E> extends AbstractQueue<E> implements Queue<E>
{
    private static final int DEFAULT_INITIAL_CAPACITY = 100;
    private Object[] arr;
    private int count;
    private int first;
    
    /**
     * @param capacity
     *      initial capacity
     */
    public DefaultQueue(int capacity)
    {
        arr = new Object[capacity];
        count = 0;
    }

    /**
     */
    public DefaultQueue()
    {
        arr = new Object[DEFAULT_INITIAL_CAPACITY];
        count = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addAll(Collection<? extends E> c)
    {
        boolean added = false;

        for (E e : c)
            if (add(e))
                added = true;

        return added;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return Arrays.toString(arr);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeAll(Collection<?> c)
    {
        boolean removed = !isEmpty();
        clear();
        return removed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsAll(Collection<?> c)
    {
        for (Object o : c)
            if (!contains(o))
                return false;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(Object o)
    {
        if (!contains(o))
            return false;
        else
            throw new RuntimeException("not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty()
    {
        final boolean empty = count == 0;
        if (empty) first = 0;
        return empty;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean retainAll(Collection<?> c)
    {
        throw new RuntimeException("not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object[] toArray()
    {
        return Arrays.<Object>copyOf(arr, count);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T[] toArray(T[] array)
    {
        throw new RuntimeException("not implemented yet");
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean offer(E e)
    {
        return add(e);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size()
    {
        return count;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(Object o)
    {
        for (Object e : arr)
            if (o.equals(e)) return true;

        return false;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean add(E elt)
    {
        ensureCapacity();
        setInternal(count, elt);
        ++count;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear()
    {
        while (!isEmpty()) {
            remove();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<E> iterator()
    {
        @SuppressWarnings("unchecked")
        E[] e = (E[]) arr;
        return Arrays.asList(e).iterator();
    }

    /**
     * Ensure space for at least one more element, roughly
     * doubling the capacity each time the array needs to grow.
     */
    private void ensureCapacity()
    {
        if (count == arr.length) {
            Object[] arr2 = new Object[arr.length * 2];
            for (int i = 0; i < count; i++) {
                arr2[i] = getInternal(i);
            }
            first = 0;
            arr = arr2;
        }
    }

    // CHECKSTYLE:OFF
    @Override
    public E poll()
    {
        return remove();
    }

    @Override
    public E remove()
    {
        if (count == 0)
            return null;
        
        E ret = getInternal(0);
        setInternal(0, null); // this must be called to reset the "first" attribute.
        first = (first+1) % arr.length;
        --count;
        
        return ret;
    }

    @Override
    public E element()
    {
        return peek();
    }
    
    @Override
    public E peek()
    {
        if (count == 0)
            return null;
        
        return getInternal(0);
    }
    
    public E fetch(int i)
    {
        if (i >= count)
            throw new ArrayIndexOutOfBoundsException();
        return getInternal(i);
    }
    
    public void replace(int i, E elt)
    {
        if (i >= count)
            throw new ArrayIndexOutOfBoundsException();
        setInternal(i, elt);
    }
    
    public int count()
    {
        return count;
    }

    private E getInternal(int i)
    {
        //noinspection RedundantTypeArguments
        return Objects.<E>as(arr[(first+i) % arr.length]);
    }

    private void setInternal(int i, E elt)
    {
        arr[(first+i) % arr.length] = elt;
    }
    // CHECKSTYLE:ON
}
