/* ************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.spi;

import java.util.Map;
import java.util.HashMap;
import java.util.NoSuchElementException;

/**
 * A binary tree implementation
 */
public class BinaryTree<T extends Comparable<? super T>>
{
    protected Entry<T>        root;
    protected Map<T,Entry<T>> elements;
    protected int             size;

    /** denotes a left child */
    public static final int LEFT = 0;

    /** denotes a right child */
    public static final int RIGHT = 1;

    /**
     * Builds a tree with the given argument as root
     *
     * @param root
     *     the root of the tree
     */
    public BinaryTree( T root ) {
        this.root     = new Entry<T>(root);
        this.elements = new HashMap<T,Entry<T>>();
        this.size     = 1;

        elements.put( root, this.root );
    }

    /* 
     * methods that may be needed later (YAGNI!):
     *   insert(T) => insert(T, Entry<T>)
     *   remove(T) => remove(T, Entry<T>)
     *   findMin(Entry<T>)
     *   findMax(Entry<T>)
     *   removeInorderNext(Entry<T>)
     *   removeInorderPrev(Entry<T>)
     *   removePreorderNext(Entry<T>)
     *   removePreorderPrev(Entry<T>)
     *   removePostorderNext(Entry<T>)
     *   removePostorderPrev(Entry<T>)
     */

    /**
     * returns the root element.
     *
     * @return
     *     element at the root
     */
    public T getRootElement() {
        return root.element;
    }

    /**
     * whether or not the tree contains the given value.
     *
     * @param value
     *     element searched in the tree
     * @return
     *     {@code true} if the element is contained; {@code false} otherwise.
     */
    public boolean contains(T value) {
        return valueOf(find(value, root)) != null;
    }

    /**
     * returns the value contained in the given entry
     *
     * @param entry
     *     entry whose value is extracted from
     * @return
     *     the corresponding element; {@code null} if entry is {@code null}
     */
    private T valueOf(Entry<T> entry) {
        return entry == null ? null : entry.element;
    }

    /**
     * Sets a value as the parent of another given child value.
     *
     * @param parentValue
     *     the value that will be the parent of {@code childValue}
     * @param childValue
     *     the value that will be the child of {@code parentValue}
     * @param leftOrRight
     *     whether the child will be at the left or the right of the parent
     * @throws NoSuchElementException
     *     if parentValue isn't a member of the tree
     * @throws IllegalArgumentException
     *     if the {@code leftOrRight} parameter isn't {@link LEFT} or {@link RIGHT}; if {@code 
     *     childValue} is already in the tree; if {@code parentValue} already has a child in the 
     *     given position (left or right).
     */
    public Entry<T> setChild( T parentValue, T childValue, int leftOrRight ) {

        if(leftOrRight != LEFT && leftOrRight != RIGHT) {
            throw new IllegalArgumentException( leftOrRight + " not a valid child position");
        }

        if(elements.get(childValue) != null) {
            throw new IllegalArgumentException("Child value already in tree");
        }

        Entry<T> parentEntry;
        Entry<T> childEntry;

        parentEntry = elements.get(parentValue);

        if( parentEntry == null ) {
            throw new NoSuchElementException(parentValue + " not in tree");
        }

        if( leftOrRight == LEFT ) {
            childEntry = parentEntry.left;
        } else {
            childEntry = parentEntry.right;
        }

        if(childEntry != null) {
            throw new IllegalArgumentException("Parent already has child");
        }

        childEntry = new Entry<T>(childValue);

        elements.put(childValue, childEntry);

        if( leftOrRight == LEFT ) {
            parentEntry.left = childEntry;
        } else {
            parentEntry.right = childEntry;
        }

        size++;

        return childEntry;
    }

    /**
     * returns the number of elements in the tree
     *
     * @return size of the tree.
     */
    public int size() {
        return size;
    }

    /**
     * Finds recursively for a value given the root of a sub-tree where the value is expected to be 
     * found.
     *
     * @param value
     *     value whose entry is being searched for
     * @param value
     *     root of the sub-tree where the value is being looked for
     * @return
     *     the value if found; null, otherwise
     */
    private Entry<T> find(T value, Entry<T> entry) {
        while (entry != null) {
            if (value.compareTo(entry.element) < 0)
                entry = entry.left;
            else if (value.compareTo(entry.element) > 0)
                entry = entry.right;
            else
                return entry;
        }

        return null;
    }

    /**
     * returns the string representation of the sub-tree rooted at {@code entry}.
     *
     * @param entry
     *     root of the sub-tree
     * @param padding
     *     the string used to pad the result
     * @return
     *     string representation of the sub-tree
     */
    private String toString(Entry<T> entry, String padding) {
        String str = "";

        if (entry != null) {
            str += padding + entry.element + "\n";
            str += toString(entry.left,  padding + padding );
            str += toString(entry.right, padding + padding );
        }

        return str;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return toString(root, "");
    }

    /**
     * An entry of the binary tree
     */
    public static class Entry<T extends Comparable<? super T>>
    {
        T        element;
        Entry<T> left;
        Entry<T> right;

        /**
         * creates a binary tree entry
         *
         * @param element
         *     element to be wrapped by this entry
         */
        public Entry(T element)
        {
            this.element = element;
            this.left    = null;
            this.right   = null;
        }

        /**
         * Returns the corresponding element
         *
         * @return the element wrapped by the entry
         */
        public T getElement()
        {
            return element;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return element.toString();
        }
    }
}
