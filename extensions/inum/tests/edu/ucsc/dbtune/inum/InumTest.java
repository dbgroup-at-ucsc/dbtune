package edu.ucsc.dbtune.inum;

import com.google.common.collect.Sets;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.SharedFixtures;
import edu.ucsc.dbtune.util.Combinations;
import java.util.Set;
import org.hamcrest.CoreMatchers;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Test the {@link Inum} class.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumTest {
  @Test public void testInumSpaceGeneration() throws Exception {
    final Inum inum = SharedFixtures.configureInum();
    inum.start();
    assertThat(inum.getInumSpace(), CoreMatchers.<Object>notNullValue());
    assertThat(inum.getInumSpace().getAllSavedOptimalPlans().isEmpty(), is(false));
  }

  @Test public void testCombinationGeneration() throws Exception {
    final Set<DummyIndex> indexes = Sets.newHashSet();
    for(int idx = 0; idx < 3; idx++){
      indexes.add(new DummyIndex(SharedFixtures.configureIndex("create index " + idx)));
    }

    final Set<Set<DummyIndex>> combinations = Combinations.findCombinations(indexes);
    assertThat(combinations.size(), equalTo(8));
  }

  @Test (expected = InumExecutionException.class) public void testInumShutdown() throws Exception {
    final Inum inum = SharedFixtures.configureInum();
    inum.end();
    inum.estimateCost("lalala", Sets.<DBIndex>newHashSet());
    fail("if we reached this line...then test has failed");
  }

  private static class DummyIndex implements
      Comparable<DummyIndex> /*comparable? why? just because we want to be able to sort them....*/ {
    private final DBIndex index;

    DummyIndex(DBIndex index){
      this.index = index;
    }

    @Override public int compareTo(DummyIndex comparableIndex) {
      return index.creationText().compareTo(comparableIndex.index.creationText());
    }
  }
}
