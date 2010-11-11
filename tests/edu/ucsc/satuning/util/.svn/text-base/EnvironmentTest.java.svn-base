package edu.ucsc.satuning.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class EnvironmentTest {
    private Environment environment;

    @Before
    public void setUp() throws Exception {
        environment = Environment.defaultEnvironment();
    }
    @Test
    public void testLoadingPGTestsDir() throws Exception {
        String path = environment.getPgDirectoryPath();
        final File pgTestDir = new File(path);
        assertNotNull("pgtests dir must exists", pgTestDir);
        assertTrue("pgtests is dir", pgTestDir.isDirectory());
    }

    @Test
    public void testLoadingAdvisDir() throws Exception {
        String path = environment.getDB2AdvisPath();

        final File advisDir = new File(path);
        assertFalse("it does not exists", advisDir.exists());
    }

    @After
    public void tearDown() throws Exception {
        environment = null;
    }
}
