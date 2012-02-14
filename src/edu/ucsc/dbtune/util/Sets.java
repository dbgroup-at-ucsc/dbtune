package edu.ucsc.dbtune.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 
 * @author Quoc Trung Tran
 *
 */
public class Sets<E> 
{
    private List<E> list;
  
    public Sets()
    {
        
    }
    
    /**
     * Enumerate all subsets of the given {@code set}, where the number of elements in each
     * enumerated set is equal to the given {@code size} value.
     * 
     * @param set
     *      A set of elements
     * @param size
     *      The number of elements in every enumerated subset. If {@code size} is greater than
     *      the number of element in {@code set}, then {@code size} is automatically adjusted to be 
     *      the number of elements in {@code set}.
     *      
     * @return
     *      The power set of all subsets of the given size. 
     */
    public Set<Set<E>> powerSet(Set<E> set, int size)
    {
        Set<Set<E>> result = new HashSet<Set<E>>();
        list = new ArrayList<E>(set);
        
        if (size > set.size())
            size = set.size();
        
        for (int pos = 0; pos < list.size(); pos++)
            result.addAll(combinations(pos, size));
        
        return result;
    }
    
    /**
     * Enumerate all subsets of size {@code size} that contains the element {@list[pos]}
     * 
     * @param pos
     *      The position of element that is included in every subset
     * @param size
     *      The number of elements in the enumerate subset
     *      
     * @return
     *      The power set of all sets of the given size.
     */
    private Set<Set<E>> combinations(int pos, int size)
    {
        Set<Set<E>> result = new HashSet<Set<E>>();
        
        E ele = list.get(pos);
        
        // base case: return an empty set 
        // when the number of elements from the position {@code pos}
        // is not enough to account for {@code size} elements.
        if (pos + size > list.size())
            return result;
        
        // another base case
        if (size == 1) {
            Set<E> set = new HashSet<E>();
            set.add(ele);
            result.add(set);
            return result;
        }
        
        // for every element of list starting from pos
        for (int i = pos + 1; i < list.size(); i++) {
            
            for (Set<E> set : combinations(i, size - 1)) {
                set.add(ele);
                result.add(set);
            }
        }
        
        return result;
    }
}
