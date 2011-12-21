package edu.ucsc.dbtune.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class Combinations
{
    private Combinations()
    {
    }

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
    public static <T> Set<Set<T>> findCrossProduct(Iterable<T> elements, int n)
    {
        final Set<Set<T>> result = Sets.newHashSet();
        if (n == 0) {
            result.add(Sets.<T>newHashSet());
            return result;
        }

        final Set<Set<T>> crossProducts = findCrossProduct(elements, n - 1);

        for (Set<T> crossproduct: crossProducts){
            for (T element: elements){
                if (crossproduct.contains(element)) { continue; }
                final Set<T> list = Sets.newHashSet();
                list.addAll(crossproduct);
                if (list.contains(element))        { continue; }
                list.add(element);

                if (result.contains(list))         { continue; }
                result.add(list);
            }
        }

        return result;
    }

    public static Set<Set<Index>> findCrossProduct(Set<Index> source)
    {
        Set<Set<Index>>  result = Sets.newHashSet();

        for (int i = 0; i <= source.size(); i++){
            result.addAll(findCrossProduct(source, i));
        }

        final Set<Set<Index>> combinations = Sets.newHashSet();

        for (Set<Index> each : result){
            combinations.add(new HashSet<Index>(Lists.<Index>newArrayList(each)));
        }

        return combinations;
    }
}
