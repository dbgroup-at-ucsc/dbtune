package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.util.DefaultQueue;

/**
 * @author Karl Schnaitter
 */
public class IBGNodeQueue
{
    private final DefaultQueue<Object> queue;

    /**
     * construct a new {@link IBGNodeQueue} object.
     */
    public IBGNodeQueue()
    {
        this(new DefaultQueue<Object>());
    }

    /**
     * construct a new {@link IBGNodeQueue} object given an initial {@link DefaultQueue queue}
     * of values.
     * @param queue
     *      a queue of values that will populate the {@link IBGNodeQueue} object.
     */
    IBGNodeQueue(DefaultQueue<Object> queue)
    {
        this.queue = queue;
    }

    /**
     * add child and all siblings (following next pointers) to the queue.
     *
     * @param ch
     *      new child node.
     */
    public final void addChildren(IndexBenefitGraph.Node.Child ch)
    {
        if (ch != null) queue.add(ch);
    }

    /**
     * add a single node to the queue.
     *
     * @param node
     *      new node to be added.
     */
    public final void addNode(IndexBenefitGraph.Node node)
    {
        queue.add(node);
    }

    /**
     * test if the queue has something remaining.
     *
     * @return
     *      {@code true} if the queue has something remaining.
     */
    public final boolean hasNext()
    {
        return !queue.isEmpty();
    }


    /**
     * remove all queued nodes.
     */
    public final void clear()
    {
        queue.clear();
    }


    /**
     * @return the next node, or return null if none
     */
    public final IndexBenefitGraph.Node next()
    {
        if (queue.isEmpty()) {
            return null;
        }
        
        Object obj = queue.peek();
        if (obj instanceof IndexBenefitGraph.Node.Child) {
            IndexBenefitGraph.Node.Child child = (IndexBenefitGraph.Node.Child) obj;

            if (child.getNext() != null) {
                queue.replace(0, child.getNext());
            } else {
                queue.remove();
            }

            return child.getNode();
        } else {
            queue.remove();
            return (IndexBenefitGraph.Node) obj;
        }
    }

    /**
     * @return the top node in queue without removing it from the queue.
     */
    public IndexBenefitGraph.Node peek()
    {
        if (queue.isEmpty()) {
            return null;
        }
        
        Object obj = queue.peek();

        if (obj instanceof IndexBenefitGraph.Node.Child) {
            IndexBenefitGraph.Node.Child child = (IndexBenefitGraph.Node.Child) obj;
            return child.getNode();
        } else {
            return (IndexBenefitGraph.Node) obj;
        }
    }
}
