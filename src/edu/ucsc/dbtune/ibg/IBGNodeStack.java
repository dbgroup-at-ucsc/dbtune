/* **************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                         *
 *                                                                              *
 *   Licensed under the Apache License, Version 2.0 (the "License");            *
 *   you may not use this file except in compliance with the License.           *
 *   You may obtain a copy of the License at                                    *
 *                                                                              *
 *       http://www.apache.org/licenses/LICENSE-2.0                             *
 *                                                                              *
 *   Unless required by applicable law or agreed to in writing, software        *
 *   distributed under the License is distributed on an "AS IS" BASIS,          *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 *   See the License for the specific language governing permissions and        *
 *   limitations under the License.                                             *
 * **************************************************************************** */
package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.ibg.IBGNode;
import edu.ucsc.dbtune.ibg.IBGNode.IBGChild;
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
