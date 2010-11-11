/*
 * ****************************************************************************
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
 *  ****************************************************************************
 */

package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGChild;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.Queue;
import edu.ucsc.dbtune.util.ToStringBuilder;

public class IBGNodeQueue {
	private final Queue<Object> queue;
    public IBGNodeQueue(){
        this(new Queue<Object>());
    }

    IBGNodeQueue(Queue<Object> queue){
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
			IBGChild child = (IBGChild) obj	;
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
			IBGChild child = (IBGChild) obj	;
			return child.node;
		} else {
			return (IBGNode) obj;
		}
	}

    @Override
    public String toString() {
        return new ToStringBuilder<IBGNodeQueue>(this)
               .add("queue", queue)
               .toString();
    }
}
