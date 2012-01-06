package edu.ucsc.dbtune.util;

import java.util.Stack;

import org.junit.Test;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for Stack.
 *
 * @author Ivo Jimenez
 */
public class StackTest
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
     * Ensures that a Stack behaves OK when calling the {Stack#empty} method.
     *
     * @param tested
     *      the Stack being tested. It's initial capacity should be one and be empty.
     * @param element
     *      element not contained in the Stack
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkEmpty(Stack<E> tested, E element)
    {
        assertThat(tested.empty(), is(true));
        tested.push(element);
        assertThat(tested.empty(), is(false));
    }

    /**
     * Ensures that a Stack behaves OK when calling the {Stack#peek} method.
     *
     * @param tested
     *      the Stack being tested.
     * @param element
     *      element not contained in the Stack
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkPeek(Stack<E> tested, E element)
    {
        assertThat(tested.push(element), is(element));
        assertThat(tested.peek(), is(element));
        assertThat(tested.peek(), is(element));
        assertThat(tested.pop(), is(element));
        assertThat(tested.empty(), is(true));
    }

    /**
     * Ensures that a Stack behaves OK when calling the {Stack#pop} method.
     *
     * @param tested
     *      the Stack being tested. It's initial capacity should be one and should be empty.
     * @param element
     *      element not contained in the Stack
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkPop(Stack<E> tested, E element)
    {
        assertThat(tested.push(element), is(element));
        assertThat(tested.pop(), is(element));
        assertThat(tested.empty(), is(true));
        assertThat(tested.push(element), is(element));
        assertThat(tested.push(element), is(element));
        assertThat(tested.push(element), is(element));
        assertThat(tested.push(element), is(element));
        assertThat(tested.push(element), is(element));
        assertThat(tested.push(element), is(element));
        assertThat(tested.push(element), is(element));
        tested.clear();
        assertThat(tested.empty(), is(true));
    }

    /**
     * Ensures that a Stack behaves OK when calling the {Stack#push} method.
     *
     * @param tested
     *      the Stack being tested. It's initial capacity should be one and be empty.
     * @param element
     *      element not contained in the Stack
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkPush(Stack<E> tested, E element)
    {
        assertThat(tested.push(element), is(element));
        assertThat(tested.pop(), is(element));
    }

    /**
     * Ensures that a Stack behaves OK when calling the {Stack#push} method.
     *
     * @param tested
     *      the Stack being tested. It's initial capacity should be one and be empty.
     * @param element
     *      element not contained in the Stack
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkSearch(Stack<E> tested, E element)
    {
        assertThat(tested.push(element), is(element));
        assertThat(tested.search(element), is(greaterThanOrEqualTo(1)));
        assertThat(tested.pop(), is(element));
        assertThat(tested.search(element), is(-1));
    }
}
