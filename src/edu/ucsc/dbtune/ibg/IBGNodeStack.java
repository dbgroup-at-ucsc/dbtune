package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode.IBGChild;
import edu.ucsc.dbtune.util.DefaultStack;

public class IBGNodeStack
{
    private final DefaultStack<Object> stack;

    /**
     * construct a new {@link IBGNodeStack} object.
     */
    public IBGNodeStack(){
        this(new DefaultStack<Object>());
    }

    /**
     * construct a new {@link IBGNodeStack} object given an initial {@link DefaultStack Stack}
     * of values.
     * @param stack
     *      a stack of values that will populate the {@link IBGNodeStack} object.
     */
    IBGNodeStack(DefaultStack<Object> stack){
        this.stack = stack;
    }

    /**
     * add child and all siblings (following next pointers) to the stack
     * @param ch
     *      new child to be added.
     */
    final void addChildren(IBGChild ch) {
        if (ch != null) stack.push(ch);
    }

    /**
     * add a new ibg node to the stack.
     * @param node
     *      node to be added to the stack.
     */
    final void addNode(IBGNode node) {
        stack.push(node);
    }

    /**
     * test if the stack has something remaining
     * @return
     *      {@code true} if the stack has something remaining.
     */
    final boolean hasNext() {
        return !stack.isEmpty();
    }

    /**
     * remove all stack nodes.
     */
    final void reset() {
        stack.popAll();
    }

    /**
     * @return the next node, or return null if none
     */
    final IBGNode next() {
        if (stack.isEmpty()) return null;
        
        Object obj = stack.peek();
        if (obj instanceof IBGChild) {
            IBGChild child = (IBGChild) stack.peek();
            if (child.next != null) {
                stack.swap(child.next);
            } else {
                stack.pop();
            }

            return child.node;
        } else {
            stack.pop();
            return (IBGNode) obj;
        }
    }
}
