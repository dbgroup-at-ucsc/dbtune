package edu.ucsc.dbtune.inum;

import java.io.File;
import java.util.Set;
import static org.hamcrest.CoreMatchers.is;
import org.junit.AfterClass;
import static org.junit.Assert.assertThat;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the loading of workload files needed for setting up Inum.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class WorkloadVisitorTest {
  private static boolean IS_EMPTY;
  @BeforeClass public static void setUp() throws Exception {
    IS_EMPTY = new File(WorkloadDirectoryNode.INUM_WORKLOAD_PATH).listFiles().length == 0;
  }

  @Test public void testWorkloadLoading() throws Exception {
    final WorkloadDirectoryNode directory = new WorkloadDirectoryNode();
    final Set<String> workload = directory.accept(new SetupWorkloadVisitor());
    assertThat(workload.isEmpty(), is(IS_EMPTY));
  }

  @AfterClass public static void tearDown() throws Exception {
    IS_EMPTY = false;
  }
}
