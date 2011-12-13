package edu.ucsc.dbtune.util;

import java.util.NoSuchElementException;

/**
 * A binary tree implementation
 */
public class BinaryTree<T extends Comparable<? super T>> extends Tree<T>
{
    /** denotes a left child */
    public static final int LEFT = 0;

    /** denotes a right child */
    public static final int RIGHT = 1;

    /**
     * Creates a binary tree with the given root.
     *
     * @param root
     *     root of the tree
     */
    public BinaryTree( T root ) {
        super(root);
    }

    /* 
     * methods that may be needed later (YAGNI!):
     *   insert(T) => insert(T, Entry<T>)
     *   remove(T) => remove(T, Entry<T>)
     *   findMin(Entry<T>)
     *   findMax(Entry<T>)
     *   iteratorInorder()
     *   iteratorPreorder()
     *   iteratorPostorder()
     *   removeInorderNext(Entry<T>)
     *   removeInorderPrev(Entry<T>)
     *   removePreorderNext(Entry<T>)
     *   removePreorderPrev(Entry<T>)
     *   removePostorderNext(Entry<T>)
     *   removePostorderPrev(Entry<T>)
     */

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
     *     if the {@code leftOrRight} parameter isn't {@link BinaryTree#LEFT} or {@link 
     *     BinaryTree#RIGHT}; if {@code childValue} is already in the tree; if {@code parentValue} 
     *     already has a child in the given position (left or right).
     */
    public Entry<T> setChild(T parentValue, T childValue, int leftOrRight) {

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

        childEntry = null;

        if( leftOrRight == LEFT && parentEntry.children.size() > 0) {
            childEntry = parentEntry.children.get(0);
        } else if(leftOrRight == RIGHT && parentEntry.children.size() > 1) {
            childEntry = parentEntry.children.get(1);
        }

        if(childEntry != null) {
            throw new IllegalArgumentException("Parent already has child");
        }

        return super.setChild(parentValue,childValue);
    }

    /**
     * {@inheritDoc}
     */
    public Entry<T> setChild(T parentValue, T childValue) {
        Entry<T> parentEntry = elements.get(parentValue);

        if(parentEntry.children.size() > 1) {
            throw new IllegalArgumentException("Parent already has child at given sub-tree");
        }

        return setChild(parentValue,childValue);
    }

    /**
     * Finds recursively for a value given the root of a sub-tree where the value is expected to be 
     * found.
     *
     * @param value
     *     value whose entry is being searched for
     * @param entry
     *     root of the sub-tree where the value is being looked for
     * @return
     *     the value if found; null, otherwise
     */
    @Override
    protected Entry<T> find(T value, Entry<T> entry) {
        while (entry != null) {
            if (value.compareTo(entry.element) < 0) {
                if(entry.children.size() > 0) {
                    entry = entry.children.get(0);
                } else {
                    entry = null;
                }
            } else if (value.compareTo(entry.element) > 0) {
                if(entry.children.size() > 1) {
                    entry = entry.children.get(1);
                } else {
                    entry = null;
                }
            } else {
                return entry;
            }
        }

        return null;
    }
}
