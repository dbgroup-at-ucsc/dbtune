package edu.ucsc.dbtune.spi;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.ucsc.dbtune.util.Environment;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class EnvironmentTest
{
    @Before
    public void setUp() throws Exception
    {
        Environment.getInstance();
    }

    @Test
    public void testReading() throws Exception
    {
    }

    @After
    public void tearDown() throws Exception
    {
    }
}
