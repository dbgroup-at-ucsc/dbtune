package edu.ucsc.dbtune.util;

public class DefaultBlockingQueue<E> extends DefaultConcurrentQueue<E>
{
    public DefaultBlockingQueue(int capacity)
    {
        super(capacity);
    }

    public E get()
    {
        return super.get(GetOption.BLOCK);
    }

    public void put(E elt)
    {
        super.put(elt, PutOption.BLOCK);
    }
}
