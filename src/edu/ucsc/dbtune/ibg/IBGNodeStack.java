package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.util.DefaultStack;

/**
 * A stack of ibg nodes.
 *
 * @author Karl Schnaitter
 */
public class IBGNodeStack
{
    private final DefaultStack<Object> stack;

    /**
     * construct a new {@link IBGNodeStack} object.
     */
    public IBGNodeStack()
    {
        this(new DefaultStack<Object>());
    }

    /**
     * construct a new {@link IBGNodeStack} object given an initial {@link DefaultStack Stack}
     * of values.
     *
     * @param stack
     *      a stack of values that will populate the {@link IBGNodeStack} object.
     */
    IBGNodeStack(DefaultStack<Object> stack)
    {
        this.stack = stack;
    }

    /**
     * add child and all siblings (following next pointers) to the stack.
     *
     * @param ch
     *      new child to be added.
     */
    final void addChildren(IndexBenefitGraph.Node.Child ch)
    {
        if (ch != null) stack.push(ch);
    }

    /**
     * add a new ibg node to the stack.
     * @param node
     *      node to be added to the stack.
     */
    final void addNode(IndexBenefitGraph.Node node)
    {
        stack.push(node);
    }

    /**
     * test if the stack has something remaining.
     *
     * @return
     *      {@code true} if the stack has something remaining.
     */
    final boolean hasNext()
    {
        return !stack.isEmpty();
    }

    /**
     * remove all stack nodes.
     */
    final void clear()
    {
        stack.popAll();
    }

    /**
     * @return the next node, or return null if none
     */
    final IndexBenefitGraph.Node next()
    {
        if (stack.isEmpty()) return null;
        
        Object obj = stack.peek();
        if (obj instanceof IndexBenefitGraph.Node.Child) {
            IndexBenefitGraph.Node.Child child = (IndexBenefitGraph.Node.Child) stack.peek();

            if (child.getNext() != null) {
                stack.swap(child.getNext());
            } else {
                stack.pop();
            }

            return child.getNode();
        } else {
            stack.pop();
            return (IndexBenefitGraph.Node) obj;
        }
    }
}
