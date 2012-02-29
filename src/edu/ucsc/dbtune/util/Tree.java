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
public class Tree<T>
{
    protected Entry<T> root;
    protected Map<T, Entry<T>> elements;

    /**
     * Builds a tree with the given argument as root.
     *
     * @param root
     *     the root of the tree
     */
    public Tree(T root)
    {
        this.root = new Entry<T>(null, root);
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
        if (other.root == null)
            throw new RuntimeException("Root in given tree is null");

        this.root = new Entry<T>(null, other.getRootElement());

        this.elements = new HashMap<T, Entry<T>>();

        copy(root, other.root);
    }

    /**
     * Copies the subtree that hangs from {@code other} and makes it a subtree of {@code entry}. 
     * This method is used only by the copy constructor.
     *
     * @param thisParent
     *      entry whose is expanded (whose children are being populated)
     * @param otherParent
     *      another entry whose children are copied to {@code entry}
     */
    private void copy(Entry<T> thisParent, Entry<T> otherParent)
    {
        Entry<T> thisChild;

        for (Entry<T> otherChild : otherParent.children) {
            thisChild = new Entry<T>(thisParent, otherChild.element);

            thisChild.parent = thisParent;
            thisParent.children.add(thisChild);

            copy(thisChild, otherChild);
        }

        elements.put(thisParent.element, thisParent);
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
            throw new NoSuchElementException(value + " is not a member of the tree");

        List<T> children = new ArrayList<T>();

        for (Entry<T> e : entry.children)
            children.add(e.getElement());

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

        if (elements.get(childValue) != null)
            throw new IllegalArgumentException("Child value already in tree");

        parentEntry = elements.get(parentValue);

        if (parentEntry == null)
            throw new NoSuchElementException(parentValue + " not in tree");

        childEntry = new Entry<T>(parentEntry, childValue);

        parentEntry.children.add(childEntry);

        elements.put(childValue, childEntry);

        return childEntry;
    }

    /**
     * Removes the subtree corresponding to the given value.
     *
     * @param value
     *      the value whose child branch is being removed from
     * @throws NoSuchElementException
     *      if {@code value} isn't a member of the tree
     */
    public void remove(T value)
    {
        remove(elements.get(value));
    }

    /**
     * Removes an entry and the entire subtree that hangs from it.
     *
     * @param entry
     *      entry to be removed
     * @throws NoSuchElementException
     *      if entry is null
     */
    private void remove(Entry<T> entry)
    {
        if (entry == null)
            throw new NoSuchElementException("Entry not in tree");

        List<Entry<T>> subtreeEntries = new ArrayList<Entry<T>>();

        getSubtreeEntries(subtreeEntries, entry);

        for (Entry<T> e : subtreeEntries) {

            if (e.parent == null)
                root = null;
            else if (!e.parent.children.remove(e))
                throw new RuntimeException("Can't remove " + e.element);

            e.parent = null;
            elements.remove(e.element);
        }
    }

    /**
     * Returns an entry and its subtree in a list.
     *
     * @param entries
     *      where to put the entries that hang from {@code entry}
     * @param entry
     *      entry whose subtree is added to {@code entries}
     */
    private void getSubtreeEntries(List<Entry<T>> entries, Entry<T> entry)
    {
        for (Entry<T> child : entry.getChildren())
            getSubtreeEntries(entries, child);

        entries.add(entry);
    }

    /**
     * Returns the parent of the given value.
     *
     * @param childValue
     *      the value that will be the child of the returned parent value
     * @return
     *      parent of {@code child}; {@code null} if {@code child} is the root or not in the tree
     */
    public T getParent(T childValue)
    {
        Entry<T> childEntry = find(childValue, root);

        if (childEntry == null)
            throw new NoSuchElementException("Value " + childValue + " not in tree");

        if (childEntry.parent == null)
            // the root, so no parent
            return null;

        return childEntry.parent.getElement();
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
     * Finds recursively (in a breath-first manner) for a value given the root of a sub-tree where 
     * the value is expected to be found. Uses the {@link Object#equals} method.
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
        if (value.equals(entry.element))
            return entry;

        Entry<T> found;

        for (Entry<T> e : entry.children) {
            found = find(value, e);

            if (found != null) {
                return found;
            }
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return root.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
    
        if (!(obj instanceof Tree<?>))
            return false;
    
        @SuppressWarnings("unchecked")
        Tree<T> o = (Tree<T>) obj;

        return root.equals(o.root);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return root.print("", true);
    }

    /**
     * An entry of the tree.
     */
    public static class Entry<T>
    {
        private T element;
        private List<Entry<T>> children;
        private Entry<T> parent;

        /**
         * creates a tree entry.
         *
         * @param parent
         *     the new element's parent entry
         * @param element
         *     element to be wrapped by this entry
         */
        public Entry(Entry<T> parent, T element)
        {
            this.parent   = parent;
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
         * Gets the parent for this instance.
         *
         * @return The parent.
         */
        public Entry<T> getParent()
        {
            return this.parent;
        }

        /**
         * @param prefix
         *      prefix
         * @param isTail
         *      if it's tail
         * @return
         *      the string of the subtree hanging at this node
         */
        private String print(String prefix, boolean isTail)
        {
            StringBuilder sb = new StringBuilder();

            if (parent == null)
                sb.append("   " + element + "\n");
            else
                sb.append(prefix + (isTail ? "└── " : "├── ") + element + "\n");

            for (int i = 0; i < children.size() - 1; i++)
                sb.append(children.get(i).print(prefix + (isTail ? "    " : "│   "), false));

            if (children.size() >= 1)
                sb.append(
                        children.get(
                            children.size() - 1).print(prefix + (isTail ? "    " : "│   "), true));

            return sb.toString();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode()
        {
            int code = 1;

            code = 37 * code + element.hashCode();

            if (parent != null)
                code = 37 * code + parent.getElement().hashCode();

            code = 37 * code + children.hashCode();

            return code;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;

            if (!(obj instanceof Entry<?>))
                return false;

            @SuppressWarnings("unchecked")
            Entry<T> o = (Entry<T>) obj;

            if (children.size() != o.children.size() ||
                    (parent != null && o.parent == null) ||
                    (parent == null && o.parent != null) ||
                    (parent != null && o.parent != null && 
                     !parent.getElement().equals(o.parent.getElement())) ||
                    !element.equals(o.element))
                return false;

            for (int i = 0; i < children.size(); i++)
                if (!children.get(i).equals(o.children.get(i)))
                    return false;

            return true;
        }
    }
}
