package edu.ucsc.dbtune.spi;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class EnvironmentTest {
    private Environment environment;
    @Before
    public void setUp() throws Exception {
        environment = Environment.getInstance();
    }

    @Test
    public void testDefaultProperties() throws Exception{
        assertThat(environment.getDatabaseName(), equalTo("test"));
        assertThat(environment.getUsername(),     equalTo("dbtune"));
        assertThat(environment.getPassword(),     equalTo("dbtuneadmin"));
    }

    @After
    public void tearDown() throws Exception {
        environment = null;
    }
}

