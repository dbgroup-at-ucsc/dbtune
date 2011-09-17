package edu.ucsc.dbtune.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import java.util.Set;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class Combinations {
  private Combinations(){}

  /**
   * recursively finds all possible combinations of interesting orders, given a length of the combination,
   * that could be covered in a query. Indexes should implement the Comparable type before they
   * could be enumerated with this method.
   * @param elements
   *    the source to be combined in many ways.
   * @param n
   *    length
   * @param <T>
   *      a type bound.
   * @return
   *    a list of all combinations for the given elements.
   */
  public static <T> Set<Set<T>> findCombinations(Iterable<T> elements, int n){
    final Set<Set<T>> result = Sets.newHashSet();
    if(n == 0) {
      result.add(Sets.<T>newHashSet());
      return result;
    }

    final Set<Set<T>> combinations = findCombinations(elements, n - 1);

    for (Set<T> combination: combinations){
      for (T element: elements){
        if (combination.contains(element)) { continue; }
        final Set<T> list = Sets.newHashSet();
        list.addAll(combination);
        if (list.contains(element))        { continue; }
        list.add(element);

        if (result.contains(list))         { continue; }
        result.add(list);
      }
    }

    return result;
	}

  @Deprecated public static <T> Set<Set<T>> findCombinations(Iterable<T> elements){
    Set<Set<T>> result = Sets.newHashSet();

    final Set<T> source = Sets.newHashSet(elements);
    for (int i = 0; i <= source.size(); i++){
      result.addAll(findCombinations(elements, i));
    }
    return result;
  }

  public static Set<Configuration> findCombinations(Configuration elements){
    // todo(Huascar) too slow..not sorted...needs improvement...
    Set<Set<Index>>  result = Sets.newHashSet();
    final Set<Index> source = Sets.newHashSet(elements.toList());
    for (int i = 0; i <= source.size(); i++){
      result.addAll(findCombinations(elements.toList(), i));
    }

    final Set<Configuration> combinations = Sets.newHashSet();
    for(Set<Index> each : result){
      combinations.add(new Configuration(Lists.<Index>newArrayList(each)));
    }

    return combinations;
  }
}
