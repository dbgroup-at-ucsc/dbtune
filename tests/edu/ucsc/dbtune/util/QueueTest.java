package edu.ucsc.dbtune.util;

import java.util.Collection;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for Queue.
 *
 * @author Ivo Jimenez
 */
public class QueueTest
{
    /**
     * Empty, to avoid JUnit complains.
     */
    @Test
    public void testNothing()
    {
        assertThat(true, is(true));
    }

    /**
     * Ensures that a queue behaves OK when calling the {Queue#element} method.
     *
     * @param tested
     *      the queue being tested.
     * @param element
     *      element not contained in the queue
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkElement(Queue<E> tested, E element)
    {
        assertThat(tested.add(element), is(true));
        assertThat(tested.element(), is(element));
        assertThat(tested.element(), is(element));
        assertThat(tested.element(), is(element));
        assertThat(tested.element(), is(element));
        assertThat(tested.element(), is(element));
        assertThat(tested.remove(), is(element));
    }

    /**
     * Ensures that a queue behaves OK when calling the {Queue#offer} method.
     *
     * @param tested
     *      the queue being tested. It's initial capacity should be one and be empty.
     * @param element
     *      element not contained in the queue
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkOffer(Queue<E> tested, E element)
    {
        assertThat(tested.offer(element), is(true));
    }

    /**
     * Ensures that a queue behaves OK when calling the {Queue#peek} method.
     *
     * @param tested
     *      the queue being tested. It's initial capacity should be one and be empty.
     * @param element
     *      element not contained in the queue
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkPeek(Queue<E> tested, E element)
    {
        assertThat(tested.add(element), is(true));
        assertThat(tested.peek(), is(element));
        assertThat(tested.peek(), is(element));
        assertThat(tested.remove(), is(element));
        assertThat(tested.peek(), is((E) null));
    }

    /**
     * Ensures that a queue behaves OK when calling the {Queue#poll} method.
     *
     * @param tested
     *      the queue being tested. It's initial capacity should be one and should be empty.
     * @param element
     *      element not contained in the queue
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkPoll(Queue<E> tested, E element)
    {
        assertThat(tested.poll(), is((E) null));
        assertThat(tested.add(element), is(true));
        assertThat(tested.poll(), is(element));
    }

    /**
     * Compares the given set against other JDK implementations.
     *
     * @param tested
     *      the collection being tested
     * @param elements
     *      elements used to populate the tested set as well as the instances of JDK Sets
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkAgainstJDKQueues(Queue<E> tested, Collection<E> elements)
    {
        assertThat(tested.addAll(elements), is(true));

        Queue<E> hs = new ArrayBlockingQueue<E>(elements.size(), false, elements);

        assertThat(hs.containsAll(tested), is(true));
        assertThat(tested.containsAll(hs), is(true));

        for (E e : hs)
            assertThat(tested.contains(e), is(true));

        for (E e : tested)
            assertThat(hs.contains(e), is(true));

        //assertThat(hs.equals(tested), is(true));
        //assertThat(tested.equals(hs), is(true));

        assertThat(tested.size(), is(hs.size()));

        tested.clear();
        assertThat(tested.isEmpty(), is(true));

        ////
        
        assertThat(tested.addAll(elements), is(true));

        Queue<E> lhs = new PriorityQueue<E>(elements);

        assertThat(lhs.containsAll(tested), is(true));
        assertThat(tested.containsAll(lhs), is(true));

        for (E e : lhs)
            assertThat(tested.contains(e), is(true));

        for (E e : tested)
            assertThat(lhs.contains(e), is(true));

        //assertThat(lhs.equals(tested), is(true));
        //assertThat(tested.equals(lhs), is(true));

        assertThat(tested.size(), is(lhs.size()));
        tested.clear();
        assertThat(tested.isEmpty(), is(true));
    }
}
