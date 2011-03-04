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
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.ToStringBuilder;

public class IBGPrinter {
	private final IndexBitSet visited;
	private final IBGNodeQueue  pending;
    private static final int INITIAL_MSG_SIZE = 10000;

    /**
     * construct a new {@link IBGPrinter} object.
     */
    public IBGPrinter(){
        this(new IndexBitSet(), new IBGNodeQueue());
    }

    /**
     * construct a new {@link IBGPrinter} object given a set of visited nodes and
     * a queue of pending nodes (i.e., nodes to be visited)
     * @param visited set of visited nodes.
     * @param pending a queue of pending nodes (i.e., nodes to be visited).
     */
    IBGPrinter(IndexBitSet visited, IBGNodeQueue pending){
        this.visited = visited;
        this.pending = pending;
    }

    /**
     * prints an {@link IndexBenefitGraph} object.
     * @param ibg an {@link IndexBenefitGraph} object to be printed.
     */
	public void print(IndexBenefitGraph ibg) {
		visited.clear();
		pending.reset();
		
		pending.addNode(ibg.rootNode());
		while (pending.hasNext()) {
			IBGNode node = pending.next();

			if (visited.get(node.id)) {
                continue;
            }
			visited.set(node.id);
			
			// we don't print unexpanded nodes... 
			// we do print the used set of expanded nodes
			if (node.isExpanded()) {
				printExpanded(ibg, node);
				pending.addChildren(node.firstChild());
			}
		}
	}
	
	void printExpanded(IndexBenefitGraph ibg, IBGNode node) {
        final StringBuilder screenOutput = new StringBuilder(INITIAL_MSG_SIZE);
		boolean first;
        screenOutput.append("NODE:\t{");
		first = true;
		for (int i = node.config.nextSetBit(0); i >= 0; i = node.config.nextSetBit(i+1)) {
			if (ibg.isUsed(i)) {
				if (!first) screenOutput.append(", ");
                screenOutput.append(i);
				first = false;
			}
		}
        screenOutput.append("}\n").append("\tused {");
		first = true;
		for (IBGChild c = node.firstChild(); c != null; c = c.next) {
			if (!first) {
                screenOutput.append(", ");
			}
            screenOutput.append(c.usedIndex);
		    first = false;
		}
        screenOutput.append("}\n").append("\tcost ").append(node.cost()).append("\n\n");
		System.out.print(screenOutput.toString());
	}

    @Override
    public String toString() {
        return new ToStringBuilder<IBGPrinter>(this)
               .add("visited nodes", visited)
               .add("pending queue", pending)
              .toString();
    }
}
