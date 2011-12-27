package edu.ucsc.dbtune.util;

import java.util.Collection;
import java.util.Iterator;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for a {@link Collection}. The test assumes that another implementation of {@link 
 * Collection} has been tested, since it is used as the base for this unit test.
 *
 * @author Ivo Jimenez
 */
public final class CollectionTest
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
     * Adds all the elements in {@code reliable} to {@code tested} and checks that, after doing so, 
     * they're the same, i.e. that they contain the same number of elements after the addition.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkAdd(
            Collection<E> tested, Collection<E> reliable)
    {
        assertThat(tested.size(), is(0));

        for (E i : reliable)
            assertThat(tested.add(i), is(true));

        checkEqualsByContainment(tested, reliable);
    }

    /**
     * Adds all the elements in {@code reliable} to {@code tested} (using the {@link 
     * Collection#addAll} method) and checks that after doing so, they're the same, i.e. that they 
     * contain the same number of elements after the addition.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkAddAll(
            Collection<E> tested, Collection<E> reliable)
    {
        assertThat(tested.size(), is(0));

        assertThat(tested.addAll(reliable), is(true));

        checkEqualsByContainment(tested, reliable);
    }

    /**
     * Adds all the elements in {@code reliable} to {@code tested} (using the {@link 
     * Collection#addAll} method) and then calls {@link Collection#clear} to see if the collection 
     * is actually empty.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkClear(
            Collection<E> tested, Collection<E> reliable)
    {
        assertThat(tested.size(), is(0));

        assertThat(tested.addAll(reliable), is(true));
        tested.clear();

        assertThat(tested.size(), is(0));
    }

    /**
     * Adds all the elements in {@code reliable} to {@code tested} and then checks if the {@link 
     * Collection#contains} on each element. Relies on the {@link Collection#iterator} of the {@code 
     * tested} object.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkContains(
            Collection<E> tested, Collection<E> reliable)
    {
        assertThat(tested.size(), is(0));

        assertThat(tested.addAll(reliable), is(true));

        for (E i : tested)
            assertThat(reliable.contains(i), is(true));
    }

    /**
     * Adds all the elements in {@code reliable} to {@code tested} and then checks if the {@link 
     * Collection#containsAll}.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkContainsAll(
            Collection<E> tested, Collection<E> reliable)
    {
        assertThat(tested.size(), is(0));

        assertThat(tested.addAll(reliable), is(true));

        assertThat(tested.containsAll(reliable), is(true));
    }

    /**
     * Adds all the elements in {@code reliable} to {@code tested} and then checks if the {@link 
     * Collection#equals} method.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkEquals(
            Collection<E> tested, Collection<E> reliable)
    {
        assertThat(tested.size(), is(0));

        assertThat(tested.addAll(reliable), is(true));

        for (E i : tested)
            assertThat(reliable.contains(i), is(true));
    }

    /**
     * Checks that all the elements in the given {@code collection} are strictly contained in the 
     * given {@code set}, that is, that {@latex.inline tested $\\in$ reliable $\\land$ reliable 
     * $\\in$ tested}.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkEqualsByContainment(
            Collection<E> tested, Collection<E> reliable)
    {
        assertThat(tested.size(), is(reliable.size()));

        for (E i : reliable)
            assertThat(tested.contains(i), is(true));
    }

    /**
     * Adds all the elements in {@code reliable} to {@code tested}, calls {@link Collection#clear} 
     * and then {@link Collection#isEmpty} to see if the collection is actually empty.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkIterator(
            Collection<E> tested, Collection<E> reliable)
    {
        assertThat(tested.isEmpty(), is(true));

        assertThat(tested.addAll(reliable), is(true));

        int count = 1;

        for (Iterator<?> i = tested.iterator(); i.hasNext(); ) {
            i.remove();
            assertThat(tested.size(), is(reliable.size() - count++));
        }
    }

    /**
     * Adds all the elements in {@code reliable} to {@code tested}, calls {@link Collection#clear} 
     * and then {@link Collection#isEmpty} to see if the collection is actually empty.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkIsEmpty(
            Collection<E> tested, Collection<E> reliable)
    {
        assertThat(tested.isEmpty(), is(true));

        assertThat(tested.addAll(reliable), is(true));
        tested.clear();

        assertThat(tested.isEmpty(), is(true));
    }

    /**
     * Adds all the elements in {@code reliable} to {@code tested} and checks the {@link 
     * Collection#remove} method for each element.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkRemove(
            Collection<E> tested, Collection<E> reliable)
    {
        assertThat(tested.size(), is(0));

        tested.addAll(reliable);

        for (E i : reliable) {
            assertThat(tested.contains(i), is(true));
            assertThat(tested.remove(i), is(true));
            assertThat(tested.contains(i), is(false));
        }
    }

    /**
     * Adds all the elements in {@code reliable} to {@code tested} and checks the {@link 
     * Collection#removeAll} method.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkRemoveAll(
            Collection<E> tested, Collection<E> reliable)
    {
        assertThat(tested.size(), is(0));

        assertThat(tested.addAll(reliable), is(true));
        assertThat(tested.removeAll(reliable), is(true));

        assertThat(tested.size(), is(0));

        for (E i : reliable)
            assertThat(tested.contains(i), is(false));
    }

    /**
     * Checks the intersection (using the {@link Collection#retainAll} me elements in {@code 
     * reliable} with others in {@code tested}. The test assumes that the first element in {@code 
     * reliable} is contained only once.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param intersection
     *      a reliable collection containing an intersection
     *      test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkRetainAll(
            Collection<E> tested, Collection<E> reliable, Collection<E> intersection)
    {
        assertThat(tested.size(), is(0));
        assertThat(tested.addAll(reliable), is(true));
        assertThat(tested.retainAll(reliable), is(false));
        assertThat(tested.equals(reliable), is(true));

        assertThat(tested.retainAll(intersection), is(true));
        assertThat(tested.size(), is(intersection.size()));

        assertThat(tested.equals(intersection), is(true));
    }

    /**
     * Adds all the elements in {@code reliable} to {@code tested} and checks the {@link 
     * Collection#size} method.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkSize(
            Collection<E> tested, Collection<E> reliable)
    {
        assertThat(tested.size(), is(0));

        assertThat(tested.addAll(reliable), is(true));

        assertThat(tested.size(), is(reliable.size()));

        int count = reliable.size();

        for (E i : reliable) {
            assertThat(tested.remove(i), is(true));
            assertThat(tested.size(), is(--count));
        }
    }

    /**
     * Checks the {@link Collection#toArray} method.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkToArray(
            Collection<E> tested, Collection<E> reliable)
    {
        assertThat(tested.size(), is(0));
        assertThat(tested.addAll(reliable), is(true));
        assertThat(tested.toArray().length, is(reliable.size()));

        for (Object o : tested.toArray())
            assertThat(reliable.contains(o), is(true));
    }

    /**
     * Checks the {@link Collection#toArray(T[])} method.
     *
     * @param tested
     *      the collection being tested
     * @param reliable
     *      a reliable collection that is used to check the collection under test
     * @param <E>
     *      the type of the collections
     */
    protected static <E> void checkToArrayT(
            Collection<E> tested, Collection<E> reliable)
    {
        assertThat(tested.size(), is(0));
        assertThat(tested.addAll(reliable), is(true));
        assertThat(tested.toArray(new Object[0]).length, is(reliable.size()));

        for (Object o : tested.toArray(new Object[0]))
            assertThat(reliable.contains(o), is(true));
    }
}
