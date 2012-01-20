package edu.ucsc.dbtune.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Implementation of a tree.
 *
 * @param <T>
 *      type of objects being stored in the tree
 * @author Ivo Jimenez
 */
public class Tree<T extends Comparable<? super T>>
{
    protected Entry<T>        root;
    protected Map<T, Entry<T>> elements;

    /**
     * Builds a tree with the given argument as root.
     *
     * @param root
     *     the root of the tree
     */
    public Tree(T root)
    {
        this.root = new Entry<T>(root);
        this.elements = new HashMap<T, Entry<T>>();

        elements.put(root, this.root);
    }

    /**
     * Copy constructor.
     *
     * @param other
     *     tree being copied
     */
    public Tree(Tree<T> other)
    {
        this.elements = new HashMap<T, Entry<T>>(other.elements);

        T rootElement = other.root.element;
        this.elements.remove(other.root.element);
        this.root = new Entry<T>(rootElement);

        elements.put(rootElement, this.root);
    }

    /**
     * returns the root element.
     *
     * @return
     *     element at the root
     */
    public T getRootElement()
    {
        return root.element;
    }

    /**
     * returns the leafs of the tree.
     *
     * @return
     *     elements at the leafs
     */
    public Set<T> leafs()
    {
        // if this needs to be implemented more efficiently, we can keep a list of all the leafs and 
        // maintain it as elements are removed/inserted from/into the tree

        Set<T> leafs = new HashSet<T>();

        for (Entry<T> e : elements.values())
            if (e.children.isEmpty())
                leafs.add(e.element);

        return leafs;
    }

    /**
     * returns the children of an element.
     *
     * @param value
     *      value for which the children are being retrieved
     * @return
     *      a list containing the children elements of the given value. Empty if the given element 
     *      is a leaf of the tree.
     * @throws NoSuchElementException
     *      if {@code value} isn't a member of the tree
     */
    public List<T> getChildren(T value) throws NoSuchElementException
    {
        Entry<T> entry = find(value, root);

        if (entry == null)
            throw new NoSuchElementException(value + " is not a member");

        List<T> children = new ArrayList<T>();

        for (Entry<T> e : entry.children)
            children.add(valueOf(e));

        return children;
    }

    /**
     * returns a list with the elements. The order of the elements in the list is random, i.e. it 
     * can't be determined.
     *
     * @return
     *     list containing the elements in the tree
     */
    public List<T> toList()
    {
        return new ArrayList<T>(elements.keySet());
    }

    /**
     * whether or not the tree contains the given value.
     *
     * @param value
     *     element searched in the tree
     * @return
     *     {@code true} if the element is contained; {@code false} otherwise.
     */
    public boolean contains(T value)
    {
        return valueOf(find(value, root)) != null;
    }

    /**
     * returns the value contained in the given entry.
     *
     * @param entry
     *     entry whose value is extracted from
     * @return
     *     the corresponding element; {@code null} if entry is {@code null}
     */
    private T valueOf(Entry<T> entry)
    {
        return entry == null ? null : entry.element;
    }

    /**
     * Sets a value as the parent of another given child value.
     *
     * @param parentValue
     *      the value that will be the parent of {@code childValue}
     * @param childValue
     *      the value that will be the child of {@code parentValue}
     * @throws NoSuchElementException
     *      if parentValue isn't a member of the tree
     * @return
     *      the entry corresponding to the newly added child
     * @throws IllegalArgumentException
     *      if {@code childValue} is already in the tree.
     */
    public Entry<T> setChild(T parentValue, T childValue)
    {
        Entry<T> parentEntry;
        Entry<T> childEntry;

        if (elements.get(childValue) != null) {
            throw new IllegalArgumentException("Child value already in tree");
        }

        parentEntry = elements.get(parentValue);

        if (parentEntry == null) {
            throw new NoSuchElementException(parentValue + " not in tree");
        }

        childEntry = new Entry<T>(childValue);

        elements.put(childValue, childEntry);

        parentEntry.children.add(childEntry);

        return childEntry;
    }

    /**
     * returns the number of elements in the tree.
     *
     * @return size of the tree.
     */
    public int size()
    {
        return elements.size();
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
     *     the corresponding entry if found; null, otherwise
     */
    protected Entry<T> find(T value, Entry<T> entry)
    {
        if (value.compareTo(entry.element) == 0) {
            return entry;
        } else {
            Entry<T> found;

            for (Entry<T> e : entry.children) {
                found = find(value, e);

                if (found != null) {
                    return found;
                }
            }
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
    private String toString(Entry<T> entry, String padding)
    {
        String str = "";

        if (entry != null) {
            str += padding + entry.element + "\n";

            for (Entry<T> e : entry.children) {
                str += toString(e, padding + padding);
            }
        }

        return str;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return toString(root, "+");
    }

    /**
     * An entry of the tree.
     */
    public static class Entry<T extends Comparable<? super T>>
    {
        private T element;
        private List<Entry<T>> children;

        /**
         * creates a tree entry.
         *
         * @param element
         *     element to be wrapped by this entry
         */
        public Entry(T element)
        {
            this.element  = element;
            this.children = new ArrayList<Entry<T>>();
        }

        /**
         * Returns the corresponding element.
         *
         * @return the element wrapped by the entry
         */
        public T getElement()
        {
            return element;
        }

        /**
         * Gets the children for this instance.
         *
         * @return The children.
         */
        public List<Entry<T>> getChildren()
        {
            return this.children;
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
