package edu.ucsc.dbtune.util;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.metadata.Index;

/**
 * Utility class for dealing with configurations.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class ConfigurationUtils {
  private ConfigurationUtils() {
  }

  /**
   * find all the used tables in a configuration.
   * @param configuration
   *    a set of indexes.
   * @return a set of used tables.
   */
  public static Set<String> findUsedTables(Set<Index> configuration) {
    final Set<String> tables = Sets.newHashSet();
    for (Index each : configuration) {
      final String name = each.getTable().getName();
      if (!tables.contains(name)) {
        tables.add(name);
      }
    }
    return tables;

  }

  /**
   * Checks if the set "a" covers the set "b". If {@code true}, then the set "b" is a subset of
   * the set "a".
   * @param a a set of indexes.
   * @param b a set of indexes.
   * @return {@code true} if second set covers the first set. {@code false} otherwise.
   */
  public static boolean isAcoveringB(Set<Index> a, Set<Index> b){
    return intersects(b, a);
  }

  private static boolean intersects(Set<Index> first, Set<Index> second){
    // second covers first, therefore first is a subset of second
    final Set<Index> c = new HashSet<Index>();
    if (first.size() < second.size()) {
      for (Index x : first) {
        if (second.contains(x)) {
          c.add(x);
        }
      }
    } else {
      for (Index x : second) {
        if (first.contains(x)) {
          c.add(x);
        }
      }
    }
    //  'first' is a subset of 'second' iff every member of 'first' is a member of 'second'
    final boolean isEveryMemberInSecond = c.size() == first.size();
    return !c.isEmpty() && isEveryMemberInSecond;
  }

}
