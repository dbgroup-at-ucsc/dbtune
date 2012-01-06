package edu.ucsc.dbtune.util;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureListOfIdentifiables;
import static edu.ucsc.dbtune.util.StackTest.checkEmpty;
import static edu.ucsc.dbtune.util.StackTest.checkPeek;
import static edu.ucsc.dbtune.util.StackTest.checkPop;
import static edu.ucsc.dbtune.util.StackTest.checkPush;

/**
 * Unit test for DefaultQueueTest.
 *
 * @author Ivo Jimenez
 */
public class DefaultStackTest
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
        // not used as such
    }

    /**
     */
    @Test
    public void testAgainstItself()
    {
        // not used as such
    }

    /**
     */
    @Test
    public void testStackMethods()
    {
        checkEmpty(new DefaultStack<Identifiable>(1), listIdentifiables.get(0));
        checkPeek(new DefaultStack<Identifiable>(1), listIdentifiables.get(0));
        checkPop(new DefaultStack<Identifiable>(1), listIdentifiables.get(0));
        checkPush(new DefaultStack<Identifiable>(1), listIdentifiables.get(0));
    }
}
