package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode.IBGChild;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.DefaultQueue;

public class IBGNodeQueue {
    private final DefaultQueue<Object> queue;

    /**
     * construct a new {@link IBGNodeQueue} object.
     */
    public IBGNodeQueue(){
        this(new DefaultQueue<Object>());
    }

    /**
     * construct a new {@link IBGNodeQueue} object given an initial {@link DefaultQueue queue}
     * of values.
     * @param queue
     *      a queue of values that will populate the {@link IBGNodeQueue} object.
     */
    IBGNodeQueue(DefaultQueue<Object> queue){
        this.queue = queue;
    }

    /**
     * add child and all siblings (following next pointers) to the queue
     * @param ch
     *      new child node.
     */
    final void addChildren(IBGChild ch) {
        if (ch != null) queue.add(ch);
    }

    /**
     * add a single node to the queue
     * @param node
     *      new node to be added.
     */
    final void addNode(IBGNode node) {
        queue.add(node);
    }

    /**
     * test if the queue has something remaining
     * @return
     *      {@code true} if the queue has something remaining.
     */
    final boolean hasNext() {
        return !queue.isEmpty();
    }


    /**
     * remove all queued nodes.
     */
    final void reset() {
        queue.clear();
    }


    /**
     * @return the next node, or return null if none
     */
    final IBGNode next() {
        if (queue.isEmpty()){
            return null;
        }
        
        Object obj = queue.peek();
        if (obj instanceof IBGChild) {
            IBGChild child = (IBGChild) obj ;
            if (child.next != null){
                queue.replace(0, child.next);
            } else {
                queue.remove();
            }
            return child.node;
        } else {
            queue.remove();
            return (IBGNode) obj;
        }
    }

    /**
     * @return the top node in queue without removing it from the queue.
     */
    public IBGNode peek() {
        if (queue.isEmpty()){
            return null;
        }
        
        Object obj = queue.peek();
        if (obj instanceof IBGChild) {
            IBGChild child = (IBGChild) obj ;
            return child.node;
        } else {
            return (IBGNode) obj;
        }
    }
}
