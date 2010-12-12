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
import edu.ucsc.dbtune.util.DefaultBitSet;

public class IBGPrinter {
	private final DefaultBitSet visited;
	private final IBGNodeQueue  pending;
    public IBGPrinter(){
        this(new DefaultBitSet(), new IBGNodeQueue());
    }

    IBGPrinter(DefaultBitSet visited, IBGNodeQueue pending){
        this.visited = visited;
        this.pending = pending;
    }
	
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
	
	private void printExpanded(IndexBenefitGraph ibg, IBGNode node) {
		boolean first;
		System.out.print("NODE:\t{");
		first = true;
		for (int i = node.config.nextSetBit(0); i >= 0; i = node.config.nextSetBit(i+1)) {
			if (ibg.isUsed(i)) {
				if (!first) System.out.print(", ");
				System.out.print(i);
				first = false;
			}
		}
		System.out.println("}");
		System.out.print("\tused {");
		first = true;
		for (IBGChild c = node.firstChild(); c != null; c = c.next) {
			if (!first) {
				System.out.print(", ");
			}
		    System.out.print(c.usedIndex);
		    first = false;
		}
		System.out.println("}");
		System.out.println("\tcost " + node.cost());
		System.out.println();
	}
}
