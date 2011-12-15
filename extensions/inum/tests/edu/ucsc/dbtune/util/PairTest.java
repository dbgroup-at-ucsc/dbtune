package edu.ucsc.dbtune.util;

import org.hamcrest.CoreMatchers;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class PairTest
{
  @Test public void testEmptyPair() throws Exception
 {
    final Pair<Object, Object> empty = Pair.empty();
    assertThat(empty.getLeft(), CoreMatchers.nullValue());
    assertThat(empty.getRight(), CoreMatchers.nullValue());
  }

  @Test public void testKeyValuePair() throws Exception
 {
    final Pair<String, Integer> keyValuePair = Pair.of("WordCount", 100);
    assertThat(keyValuePair.getLeft(), equalTo("WordCount"));
    assertThat(keyValuePair.getRight(), is(100));
  }

  @Test public void testCopyOfPairs() throws Exception
 {
    final Pair<String, Integer> keyValuePair = Pair.of("WordCount", 100);
    final Pair<String, Integer> copyKeyValuePair = Pair.copyOf(keyValuePair);
    assertThat(copyKeyValuePair.getLeft(), equalTo("WordCount"));
    assertThat(copyKeyValuePair.getRight(), is(100));
  }
}
