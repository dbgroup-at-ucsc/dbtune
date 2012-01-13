package edu.ucsc.dbtune.inum;

import java.util.Set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 * Test the loading of workload files needed for setting up Inum.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class WorkloadVisitorTest {
  @Test public void testWorkloadLoading() throws Exception {
    final WorkloadDirectoryNode directory = new WorkloadDirectoryNode();
    final Set<String> workload = directory.accept(new SetupWorkloadVisitor());
    assertThat(workload.isEmpty(), is(false));
  }
}
