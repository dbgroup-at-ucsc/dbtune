package edu.ucsc.dbtune.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for SetTest.
 *
 * @author Ivo Jimenez
 */
public class SetTest
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
     * Ensures that a set doesn't add an element more than once.
     *
     * @param tested
     *      the collection being tested
     * @param element
     *      an element used to test that it isn't added twice
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkAdd(
            Set<E> tested, E element)
    {
        assertThat(tested.contains(element), is(false));
        int sizeBefore = tested.size();
        assertThat(tested.add(element), is(true));
        assertThat(tested.size(), is(sizeBefore + 1));
        assertThat(tested.add(element), is(false));
        assertThat(tested.size(), is(sizeBefore + 1));
    }

    /**
     * Ensures that a set can't be removed "twice" (if the {@link Set#remove} method is called 
     * consecutively).
     *
     * @param tested
     *      the collection being tested
     * @param element
     *      an element used to test that it isn't added twice
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkRemove(
            Set<E> tested, E element)
    {
        assertThat(tested.contains(element), is(false));
        assertThat(tested.add(element), is(true));
        int sizeBefore = tested.size();
        assertThat(tested.remove(element), is(true));
        assertThat(tested.size(), is(sizeBefore - 1));
        assertThat(tested.remove(element), is(false));
        assertThat(tested.size(), is(sizeBefore - 1));
        assertThat(tested.remove(element), is(false));
        assertThat(tested.size(), is(sizeBefore - 1));
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
    protected static <E> void checkAgainstJDKSets(
            Set<E> tested, Collection<E> elements)
    {
        assertThat(tested.addAll(elements), is(true));

        Set<E> hs = new HashSet<E>(elements);

        assertThat(hs.containsAll(tested), is(true));
        assertThat(tested.containsAll(hs), is(true));

        for (E e : hs)
            assertThat(tested.contains(e), is(true));

        for (E e : tested)
            assertThat(hs.contains(e), is(true));

        assertThat(hs.equals(tested), is(true));
        assertThat(tested.equals(hs), is(true));

        assertThat(tested.size(), is(hs.size()));

        tested.clear();
        assertThat(tested.isEmpty(), is(true));

        ////
        
        assertThat(tested.addAll(elements), is(true));

        Set<E> lhs = new LinkedHashSet<E>(elements);

        assertThat(lhs.containsAll(tested), is(true));
        assertThat(tested.containsAll(lhs), is(true));

        for (E e : lhs)
            assertThat(tested.contains(e), is(true));

        for (E e : tested)
            assertThat(lhs.contains(e), is(true));

        assertThat(lhs.equals(tested), is(true));
        assertThat(tested.equals(lhs), is(true));

        assertThat(tested.size(), is(lhs.size()));
        tested.clear();
        assertThat(tested.isEmpty(), is(true));
    }
}
