package edu.ucsc.dbtune.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureListOfIdentifiables;
import static edu.ucsc.dbtune.util.CollectionTest.checkAdd;
import static edu.ucsc.dbtune.util.CollectionTest.checkAddAll;
import static edu.ucsc.dbtune.util.CollectionTest.checkClear;
import static edu.ucsc.dbtune.util.CollectionTest.checkContains;
import static edu.ucsc.dbtune.util.CollectionTest.checkContainsAll;
import static edu.ucsc.dbtune.util.CollectionTest.checkEquals;
import static edu.ucsc.dbtune.util.CollectionTest.checkIsEmpty;
import static edu.ucsc.dbtune.util.CollectionTest.checkRemoveAll;
import static edu.ucsc.dbtune.util.CollectionTest.checkToArray;
import static edu.ucsc.dbtune.util.QueueTest.checkAgainstJDKQueues;
import static edu.ucsc.dbtune.util.QueueTest.checkElement;
import static edu.ucsc.dbtune.util.QueueTest.checkOffer;
import static edu.ucsc.dbtune.util.QueueTest.checkPeek;
import static edu.ucsc.dbtune.util.QueueTest.checkPoll;

/**
 * Unit test for DefaultQueueTest.
 *
 * @author Ivo Jimenez
 */
public class DefaultQueueTest
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

        checkAdd(new DefaultQueue<Identifiable>(), listIdentifiables);
        checkAddAll(new DefaultQueue<Identifiable>(), listIdentifiables);
        checkClear(new DefaultQueue<Identifiable>(), listIdentifiables);
        checkContains(new DefaultQueue<Identifiable>(), listIdentifiables);
        checkContainsAll(new DefaultQueue<Identifiable>(), listIdentifiables);
        checkEquals(new DefaultQueue<Identifiable>(), listIdentifiables);
        checkIsEmpty(new DefaultQueue<Identifiable>(), listIdentifiables);
        //checkRemove(new DefaultQueue<Identifiable>(), listIdentifiables);
        checkRemoveAll(new DefaultQueue<Identifiable>(), listIdentifiables);
        //checkRetainAll(new DefaultQueue<Identifiable>(), listIdentifiables, intersection);
        //checkSize(new DefaultQueue<Identifiable>(), listIdentifiables);
        checkToArray(new DefaultQueue<Identifiable>(), listIdentifiables);
        //checkToArrayT(new DefaultQueue<Identifiable>(), listIdentifiables);
    }

    /**
     */
    @Test
    public void testAgainstItself()
    {
        // the queue is not passed as argument to itself anywhere, so no need to check
    }

    /**
     */
    @Test
    public void testQueueInterfaceMethods()
    {
        //checkAdd()
        //checkRemove()
        checkElement(new DefaultQueue<Identifiable>(1), listIdentifiables.get(0));
        checkOffer(new DefaultQueue<Identifiable>(1), listIdentifiables.get(0));
        checkPeek(new DefaultQueue<Identifiable>(1), listIdentifiables.get(0));
        checkPoll(new DefaultQueue<Identifiable>(1), listIdentifiables.get(0));
    }

    /**
     */
    @Test
    public void testAgainstOtherQueueImplementations()
    {
        checkAgainstJDKQueues(new DefaultQueue<Identifiable>(), listIdentifiables);
    }
}
