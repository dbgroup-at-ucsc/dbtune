package edu.ucsc.dbtune.util;

import java.util.ArrayList;
import java.util.List;

/**
 * The Permutations class provides an iteration of all permutations of an list
 * of objects. Each permutation is simply an ordered list of the group.
 * <p>
 * For example, to see all of the ways we can select a school representative and an 
 * alternate from a list of 4 children, begin with an array of names:: 
 * <blockquote><pre>
 *     List<Children> children = Collections.asList({Leonardo, Monica, Nathan, Olivia});
 * </pre></blockquote> 
 * To see all 2-permutations of these 4 names, create and use a Permutations enumeration: 
 * <blockquote><pre>
 * Permutations<Children> c = new Permutations<Children>(children, 2);
 * while (c.hasNext()) {
 *  List<Children> perm = c.next();
 *  for (int i = 0; i < perm.size(); i++) {
 *    System.out.print(perm.get(i) +  );
 *  }
 * System.out.println();
 * }
 * </pre></blockquote>
 * This will print out:
 * <blockquote><pre> 
 * Leonardo Monica 
 * Leonardo Nathan 
 * Leonardo Olivia 
 * Monica Leonardo 
 * Monica Nathan 
 * Monica Olivia 
 * Nathan Leonardo 
 * Nathan Monica 
 * Nathan Olivia 
 * Olivia Leonardo 
 * Olivia Monica 
 * Olivia Nathan
 * </pre></blockquote>
 * 
 */
public class  Permutations<E> implements java.util.Iterator<List<E>>{
    private List<E> inList;
    private int n, m;
    private int[] index;
    private boolean hasMore = true;

    /**
    * Create a Permutation to iterate through all possible lineups
    * of the supplied array of Objects.
    *
    * @param Object[] inArray the group to line up
    * @exception CombinatoricException Should never happen
    * with this interface
    *
    */
    public Permutations(List<E> inList){
                this(inList, inList.size());
        }
    
    /**
    * Create a Permutation to iterate through all possible lineups
    * of the supplied array of Objects.
    *
    * @param Object[] inArray the group to line up
    * @param inArray java.lang.Object[], the group to line up
    * @param m int, the number of objects to use
    * @exception CombinatoricException if m is greater than 
    * the length of inArray, or less than 0.
    */
    public Permutations(List<E> inList, int m){
        this.inList = inList;
        this.n = inList.size();
        this.m = m;

        assert this.n >= m && m >= 0;

        /**
        * index is an array of ints that keep track of the next 
        * permutation to return. For example, an index on a permutation 
        * of 3 things might contain {1 2 0}. This index will be followed 
        * by {2 0 1} and {2 1 0}.
        * Initially, the index is {0 ... n - 1}.
        */

        this.index = new int[this.n];
        for (int i = 0; i < this.n; i++) {
            this.index[i] = i;
        }

        /**
        * The elements from m to n are always kept ascending right
        * to left. This keeps the dip in the interesting region.     
        */    
        reverseAfter(m - 1);
    }
    
    /**
    * @return true, unless we have already returned the last permutation.
    */
    public boolean hasNext(){
        return this.hasMore;
    }
    
    /**
    * Move the index forward a notch. The algorithm first finds the 
    * rightmost index that is less than its neighbor to the right. This 
    * is the dip point. The algorithm next finds the least element to
    * the right of the dip that is greater than the dip. That element is
    * switched with the dip. Finally, the list of elements to the right 
    * of the dip is reversed.
    * <p>
    * For example, in a permutation of 5 items, the index may be 
    * {1, 2, 4, 3, 0}. The dip is 2  the rightmost element less 
    * than its neighbor on its right. The least element to the right of 
    * 2 that is greater than 2 is 3. These elements are swapped, 
    * yielding {1, 3, 4, 2, 0}, and the list right of the dip point is 
    * reversed, yielding {1, 3, 0, 2, 4}.
    * <p>
    * The algorithm is from Applied Combinatorics, by Alan Tucker.
    *
    */
    private void moveIndex(){
        // find the index of the first element that dips
        int i = rightmostDip();
        if (i < 0) {
            this.hasMore = false;
            return;
        }

        // find the least greater element to the right of the dip
        int leastToRightIndex = i + 1;
        for (int j = i + 2; j < this.n; j++){
            if (this.index[j] < this.index[leastToRightIndex] &&  this.index[j] > this.index[i]){
                leastToRightIndex = j;
            }
        }

        // switch dip element with least greater element to its right
        int t = this.index[i];
        this.index[i] = this.index[leastToRightIndex];
        this.index[leastToRightIndex] = t;

        if (this.m - 1 > i){
            // reverse the elements to the right of the dip
            reverseAfter(i);    
            // reverse the elements to the right of m - 1
            reverseAfter(this.m - 1);
        }
    }
    
    /**
    * @return java.lang.Object, the next permutation of the original Object array. 
    * <p>
    * Actually, an array of Objects is returned. The declaration must say just Object,
    * because the Permutations class implements Iterator, which declares that the 
    * next() returns a plain Object. 
    * Users must cast the returned object to (Object[]).
    */
    public List<E> next(){
        if (!this.hasMore){ 
            return null;
        }
        List<E> list =  new ArrayList<E>(this.m);
        for (int i = 0; i < this.m; i++){
                int thisIndexI = this.index[i];
                E element = this.inList.get(thisIndexI); 
                list.add(element);
        }
        moveIndex();
        return list;
    }
    
    /**
    * Reverse the index elements to the right of the specified index.
    */
    private void reverseAfter(int i){
        int start = i + 1;
        int end = this.n - 1;
        while (start < end){
            int t = this.index[start];
            this.index[start] = this.index[end];
            this.index[end] = t;
            start++;
            end--;
        }

    }
    
    /**
    * @return int the index of the first element from the right
    * that is less than its neighbor on the right.
    */
    private int rightmostDip(){
        for (int i = this.n - 2; i >= 0; i--){
            if (this.index[i] < this.index[i+1]){
                return i;
            }
        }
        return -1;
    }
        
    public void remove() {
                throw new UnsupportedOperationException();
        }
    
}
