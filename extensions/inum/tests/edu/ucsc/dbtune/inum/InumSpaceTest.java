package edu.ucsc.dbtune.inum;

import com.google.common.collect.Sets;
import edu.ucsc.dbtune.SharedFixtures;
import edu.ucsc.dbtune.metadata.Index;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import static junit.framework.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Tests the {@link InumSpace INUM Space} interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumSpaceTest {
  @Test public void testInumSpaceIsNotEmptyWhenConstructed() throws Exception {
    final InumSpace     space = new InMemoryInumSpace();
    final String        sql     = "Select * from Table1;";
    final Set<Index> config  = SharedFixtures.configureConfiguration();
    final QueryRecord key     = new QueryRecord(sql, config);
    final Set<OptimalPlan> plans = space.save(key, SharedFixtures.configureOptimalPlans()).getOptimalPlans(key);
    assertThat(!plans.isEmpty(), is(true));
    assertThat(!space.getAllSavedOptimalPlans().isEmpty(), is(true));
  }

  @Test public void testInumSpaceIsEmptyWhenCleared() throws Exception {
    final InumSpace     space   = new InMemoryInumSpace();
    final String        sql     = "Select * from Table1;";
    final Set<Index> config  = SharedFixtures.configureConfiguration();
    final QueryRecord key     = new QueryRecord(sql, config);
    final Set<OptimalPlan> plans = space.save(key, SharedFixtures.configureOptimalPlans()).getOptimalPlans(key);
    assertThat(!plans.isEmpty(), is(true));
    space.clear();
    assertThat(space.getAllSavedOptimalPlans().isEmpty(), is(true));
  }


  @Test public void testReadInumSpaceWhenEmpty() throws Exception {
    final InumSpace space = new InMemoryInumSpace();
    final QueryRecord record = queryRecord();
    final Thread taker = new Thread(){
      @Override public void run() {
        try {
          if(space.getOptimalPlans(record).isEmpty()){
            throw new InterruptedException("Error!");
          }

          fail(); // if we get here, it's an error.

        } catch (InterruptedException success) {
          // nothing to do here
        }
      }
    };

    try {
      taker.start();
      Thread.sleep(10000);
      taker.interrupt();
      taker.join(10000);
      assertThat(taker.isAlive(), is(false));
    } catch (Exception e){
      fail();
    }
  }

  @Test public void testReadBlocksWhenEmpty() throws Exception {
    final ExecutorService pool = Executors.newCachedThreadPool();
    new ReadSaveInumSpaceTestHelper(10, 100000, pool).testSafety();
    pool.shutdown();
  }

  private static QueryRecord queryRecord(){
    return queryRecord(0);
  }

  private static QueryRecord queryRecord(int number){
    final String extra = number == 0 ? "" : "_" + String.valueOf(number);
    return new QueryRecord("SELECT * FROM TESTS"+ extra + ";", Sets.<Index>newHashSet());
  }

  private static int xorShift(int y) {
    y ^= (y << 6);
    y ^= (y >>> 21);
    y ^= (y << 7);
    return y;
  }

  /**
   * helper class that will allows us to test thread safety of InumSpace.
   */
  static class ReadSaveInumSpaceTestHelper {

    final AtomicInteger putSum = new AtomicInteger(0);
    final AtomicInteger takeSum = new AtomicInteger(0);
    final CyclicBarrier barrier;
    final InumSpace inumSpace;
    final int trials, pairs;
    private final ExecutorService pool;

    ReadSaveInumSpaceTestHelper(int pairs, int trials, ExecutorService pool) {
      this.pool = pool;
      this.inumSpace = new InMemoryInumSpace();
      this.trials = trials;
      this.pairs = pairs;
      this.barrier = new CyclicBarrier(pairs * 2 + 1);
    }

    void testSafety() {
      try {
        for (int i = 0; i < pairs; i++) {
          pool.execute(new Producer());
          pool.execute(new Consumer());
        }
        barrier.await(); // wait for all threads to be ready
        barrier.await(); // wait for all threads to finish
        assertEquals(putSum.get(), takeSum.get());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    class Producer implements Runnable {
      public void run() {
        try {
          final QueryRecord seed = queryRecord(
              xorShift((this.hashCode() ^ (int) System.nanoTime())));
          final Set<OptimalPlan> plans = Sets.newHashSet();
          int sum = 0;
          barrier.await();
          for (int i = trials; i > 0; --i) {
            inumSpace.save(seed, plans);
            sum += inumSpace.getAllSavedOptimalPlans().size();
          }
          putSum.getAndAdd(sum);
          barrier.await();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    class Consumer implements Runnable {
      public void run() {
        try {
          barrier.await();
          int sum = 0;
          for (int i = trials; i > 0; --i) {
            sum += inumSpace.getAllSavedOptimalPlans().size();
          }
          takeSum.getAndAdd(sum);
          barrier.await();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

}
