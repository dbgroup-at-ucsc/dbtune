package edu.ucsc.dbtune.util;

import com.google.common.collect.Sets;
import edu.ucsc.dbtune.metadata.Index;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility class for dealing with the generation of all possible combinations of a set.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class Combinations {
  private Combinations() {
    throw new AssertionError("Utility class.");
  }

  /**
   * recursively finds all possible combinations of interesting orders, given a length of the
   * combination, that could be covered in a query. Indexes should implement the Comparable type
   * before they could be enumerated with this method.
   *
   * @param elements the source to be combined in many ways.
   * @param n length of each generated set. For example, let's assume we have a set containing 3
   *      elements. Given the size of this set, this method will generate 8 subsets:
   *      the empty set (n = 0), 3 singletons (n = 1), 3 subsets containing 2 elements (n = 2),
   *      and 1 subset containing 3 elements (n = 3).
   * @param <T> a type bound.
   * @return a set of all combinations for the given elements.
   */
  public static <T> Set<Set<T>> setOfAllSubsetsOfLengthN(Iterable<T> elements, int n) {
    final Set<Set<T>> result = Sets.newHashSet();
    if (n == 0) {
      result.add(Sets.<T>newHashSet());
      return result;
    }

    final Set<Set<T>> crossProducts = setOfAllSubsetsOfLengthN(elements, n - 1);

    for (Set<T> crossproduct : crossProducts) {
      for (T element : elements) {
        if (crossproduct.contains(element)) { continue; }
        final Set<T> list = Sets.newHashSet();
        list.addAll(crossproduct);
        if (list.contains(element)) { continue; }
        list.add(element);

        if (result.contains(list)) { continue; }
        result.add(list);
      }
    }

    return result;
  }

  /**
   *
   * @param elements the set to be combined in many ways.
   * @see #setOfAllSubsetsOfLengthN(Iterable, int)
   * @return a set of all combinations for the given elements.
   */
  public static Set<Set<Index>> setOfAllSubsets(Set<Index> elements) {
    Set<Set<Index>> result = Sets.newHashSet();

    for (int i = 0; i <= elements.size(); i++) {
      result.addAll(setOfAllSubsetsOfLengthN(elements, i));
    }

    final Set<Set<Index>> combinations = Sets.newHashSet();

    for (Set<Index> each : result) {
      combinations.add(new HashSet<Index>(each));
    }

    return combinations;
  }
}
