package edu.ucsc.dbtune.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Utility class for dealing with configurations.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class ConfigurationUtils {
  static final Function<Index, String> GROUPING_FUNCTION = new Function<Index, String>() {
    @Override public String apply(@Nullable Index index) {
      if(index == null) return null;
      final Table nonNullTable = Preconditions.checkNotNull(index.getTable());
      return nonNullTable.getName();
    }
  };

  private ConfigurationUtils() {
  }

  /**
   * This method returns every possible list that can be formed by choosing one element
   * from each of the given sets in order; the "n-ary Cartesian product" of the sets.
   * @param configuration
   *    a set of indexes (interesting orders)
   * @return "n-ary Cartesian product" of the sets, including the empty set.
   */
  public static Set<List<Index>> cartesianProductOf(Set<Index> configuration){
    final List<Set<Index>> grouped                           = groupIndexesByTable(configuration);
    final Set<List<Index>> carterianProductWithoutEmptyList  = Sets.cartesianProduct(grouped);
    // why a new HashSet? This is because Sets.cartesianProduct(..) returns AbstractSet object
    // which it throws an UnsupportedOperationException when invoking the add method.
    final Set<List<Index>> result = Sets.newHashSet(carterianProductWithoutEmptyList);
    result.add(Lists.<Index>newArrayList());

    return result;
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

  private static List<Set<Index>> groupIndexesByTable(Set<Index> configuration){
    final Multimap<String, Index> group = Multimaps.index(configuration, GROUPING_FUNCTION);
    final List<Set<Index>> groupedList = Lists.newArrayList();
    for(String each : group.keySet()){
      groupedList.add(Sets.<Index>newHashSet(group.get(each)));
    }
    return groupedList;
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
