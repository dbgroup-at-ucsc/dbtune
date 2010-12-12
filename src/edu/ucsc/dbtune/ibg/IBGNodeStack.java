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
import edu.ucsc.dbtune.util.DefaultStack;
import edu.ucsc.dbtune.util.ToStringBuilder;

public class IBGNodeStack {
	private final DefaultStack<Object> stack;
    public IBGNodeStack(){
        this(new DefaultStack<Object>());
    }

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

    @Override
    public String toString() {
        return new ToStringBuilder<IBGNodeStack>(this)
               .add("stack", stack)
               .toString();
    }
}
