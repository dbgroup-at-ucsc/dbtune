package edu.ucsc.dbtune.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureBitArraySetOfIdentifiables;
import static edu.ucsc.dbtune.DBTuneInstances.configureListOfIdentifiables;
import static edu.ucsc.dbtune.util.CollectionTest.checkAdd;
import static edu.ucsc.dbtune.util.CollectionTest.checkAddAll;
import static edu.ucsc.dbtune.util.CollectionTest.checkClear;
import static edu.ucsc.dbtune.util.CollectionTest.checkContains;
import static edu.ucsc.dbtune.util.CollectionTest.checkContainsAll;
import static edu.ucsc.dbtune.util.CollectionTest.checkEquals;
import static edu.ucsc.dbtune.util.CollectionTest.checkIsEmpty;
import static edu.ucsc.dbtune.util.CollectionTest.checkRemove;
import static edu.ucsc.dbtune.util.CollectionTest.checkRemoveAll;
import static edu.ucsc.dbtune.util.CollectionTest.checkRetainAll;
import static edu.ucsc.dbtune.util.CollectionTest.checkSize;
import static edu.ucsc.dbtune.util.CollectionTest.checkToArray;
import static edu.ucsc.dbtune.util.CollectionTest.checkToArrayT;
import static edu.ucsc.dbtune.util.SetTest.checkAdd;
import static edu.ucsc.dbtune.util.SetTest.checkAgainstJDKSets;
import static edu.ucsc.dbtune.util.SetTest.checkRemove;

/**
 * Unit test for BitArraySetTest.
 *
 * @author Ivo Jimenez
 */
public class BitArraySetTest
{
    private static List<Identifiable> listIdentifiables;

    /**
     * Setup for the test.
     */
    @BeforeClass
    public static void beforeClassSetUp()
    {
        listIdentifiables = configureListOfIdentifiables(100);
    }

    /**
     */
    @Test
    public void testCollectionInterfaceMethods()
    {
        List<Identifiable> intersection = new ArrayList<Identifiable>();

        intersection.add(listIdentifiables.get(0));
        intersection.add(listIdentifiables.get(1));
        intersection.add(listIdentifiables.get(3));
        intersection.add(listIdentifiables.get(5));

        checkAdd(new BitArraySet<Identifiable>(), listIdentifiables);
        checkAddAll(new BitArraySet<Identifiable>(), listIdentifiables);
        checkClear(new BitArraySet<Identifiable>(), listIdentifiables);
        checkContains(new BitArraySet<Identifiable>(), listIdentifiables);
        checkContainsAll(new BitArraySet<Identifiable>(), listIdentifiables);
        checkEquals(new BitArraySet<Identifiable>(), listIdentifiables);
        checkIsEmpty(new BitArraySet<Identifiable>(), listIdentifiables);
        checkRemove(new BitArraySet<Identifiable>(), listIdentifiables);
        checkRemoveAll(new BitArraySet<Identifiable>(), listIdentifiables);
        checkRetainAll(new BitArraySet<Identifiable>(), listIdentifiables, intersection);
        checkSize(new BitArraySet<Identifiable>(), listIdentifiables);
        checkToArray(new BitArraySet<Identifiable>(), listIdentifiables);
        checkToArrayT(new BitArraySet<Identifiable>(), listIdentifiables);
    }

    /**
     */
    @Test
    public void testAgainstItself()
    {
        Set<Identifiable> bas = configureBitArraySetOfIdentifiables(100);
        Set<Identifiable> intersection = new BitArraySet<Identifiable>();

        intersection.add(listIdentifiables.get(0));
        intersection.add(listIdentifiables.get(1));
        intersection.add(listIdentifiables.get(3));
        intersection.add(listIdentifiables.get(5));

        checkAdd(new BitArraySet<Identifiable>(), bas);
        checkAddAll(new BitArraySet<Identifiable>(), bas);
        checkClear(new BitArraySet<Identifiable>(), bas);
        checkContains(new BitArraySet<Identifiable>(), bas);
        checkContainsAll(new BitArraySet<Identifiable>(), bas);
        checkEquals(new BitArraySet<Identifiable>(), bas);
        checkIsEmpty(new BitArraySet<Identifiable>(), bas);
        checkRemove(new BitArraySet<Identifiable>(), bas);
        checkRemoveAll(new BitArraySet<Identifiable>(), bas);
        checkRetainAll(new BitArraySet<Identifiable>(), bas, intersection);
        checkSize(new BitArraySet<Identifiable>(), bas);
        checkToArray(new BitArraySet<Identifiable>(), bas);
        checkToArrayT(new BitArraySet<Identifiable>(), bas);
    }

    /**
     */
    @Test
    public void testSetInterfaceMethods()
    {
        checkAdd(new BitArraySet<Identifiable>(), listIdentifiables.get(0));
        checkRemove(new BitArraySet<Identifiable>(), listIdentifiables.get(0));
    }

    /**
     */
    @Test
    public void testAgainstOtherSetImplementations()
    {
        checkAgainstJDKSets(new BitArraySet<Identifiable>(), listIdentifiables);
    }
}
