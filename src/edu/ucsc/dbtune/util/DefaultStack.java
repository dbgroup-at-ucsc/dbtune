package edu.ucsc.dbtune.util;

import java.util.Arrays;
import java.util.Stack;

public class DefaultStack<E> extends Stack<E>
{
    static final long serialVersionUID=0;
    Object[] arr;
    int top;
    private static final int DEFAULT_INITIAL_CAPACITY = 100;

    public DefaultStack()
    {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public DefaultStack(int capacity)
    {
        arr = new Object[capacity];
        top = -1;
    }

    @Override
    public final void clear()
    {
        popAll();
    }
    
    public final void popAll()
    {
        while (!isEmpty()){
            pop();
        }
    }
    
    @Override
    public final E pop()
    {
        @SuppressWarnings("unchecked")
        E e = (E) popAndRelease();
        return e;
    }

    Object popAndRelease(){
        if (top == -1)
            throw new ArrayIndexOutOfBoundsException("cannot pop");
        final Object result = arr[top--];
        arr[top+1] = null; // Eliminate obsolete reference
        return result;
    }

    /**
     * Ensure space for at least one more element, roughly
     * doubling the capacity each time the array needs to grow.
     */
    private void ensureCapacity()
    {
        if (top == arr.length-1) {
            arr = Arrays.copyOf(arr, arr.length * 2);
        }
    }

    @Override
    public final E peek()
    {
        //noinspection RedundantTypeArguments
        return Objects.<E>as(arr[top]);
    }
    
    @Override
    public final E push(E elt)
    {
        ensureCapacity();
        arr[++top] = elt;
        return elt;
    }
    
    @Override
    public final boolean empty()
    {
        return isEmpty();
    }

    @Override
    public final boolean isEmpty()
    {
        return top == -1;
    }

    public void swap(E next)
    {
        arr[top] = next;
    }

    @Override
    public String toString()
    {
        return Arrays.toString(arr);
    }
}
