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

import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.ToStringBuilder;

/**
 * We have a bunch of structures that we keep around to avoid excessive garbage collection. These 
 * structures are only used in private method {@code 
 * edu.ucsc.dbtune.ibg.IBGAnalyzer#analyzeNode(IBGNode, InteractionLogger)}.
 *  
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class IBGAnalyzerWorkspace {
	private final IndexBitSet candidatesBitSet;
	private final IndexBitSet usedBitSet;
	private final IndexBitSet bitset_YaSimple;
	private final IndexBitSet bitset_Ya;
	private final IndexBitSet bitset_YbMinus;
	private final IndexBitSet bitset_YbPlus;
	private final IndexBitSet bitset_Yab;
    private final IBGNode           startingNode;
    private final InteractionLogger logger;
    private final IndexBitSet allUsedIndexes;
    private final IndexBitSet rootBitSet;

    public IBGAnalyzerWorkspace(
            IBGNode startingNode, InteractionLogger logger, 
            IndexBitSet rootBitSet, IndexBitSet allUsedIndexes
    ){
        this.rootBitSet         = Checks.checkNotNull(rootBitSet);
        this.allUsedIndexes     = Checks.checkNotNull(allUsedIndexes);
        this.startingNode       = Checks.checkNotNull(startingNode);
        this.logger             = Checks.checkNotNull(logger);
        candidatesBitSet        = new IndexBitSet();
        usedBitSet              = new IndexBitSet();
        bitset_YaSimple         = new IndexBitSet();
        bitset_Ya               = new IndexBitSet();
        bitset_YbMinus          = new IndexBitSet();
        bitset_YbPlus           = new IndexBitSet();
        bitset_Yab              = new IndexBitSet();
    }
    
	/**
	 * Compute the interaction level based on the four costs
	 *
	 *     | C - C_a - C_b + C_ab |
	 */
	@SuppressWarnings({"JavaDoc"})
    private static double interactionLevel(double empty, double a, double b, double ab) {
		return Math.abs(empty - a - b + ab);
	}


    public boolean runAnalysis(IBGCoveringNodeFinder coveringNodeFinder, IndexBenefitGraphConstructor ibgCons){
        IndexBitSet bitset_Y = startingNode.config;
        updateUsedSet();
        storeUsedSet(allUsedIndexes);
        setUpCandidates();
        return traverseCandidatePool(coveringNodeFinder, ibgCons, bitset_Y);
    }

    public void storeUsedSet(IndexBitSet allUsedIndexes){
		// store the used set
		allUsedIndexes.or(usedBitSet);
    }

    public void setUpCandidates(){
		// set up candidates
		candidatesBitSet.set(rootBitSet);
		candidatesBitSet.andNot(usedBitSet);
		candidatesBitSet.and(allUsedIndexes);
    }

    private boolean traverseCandidatePool(IBGCoveringNodeFinder coveringNodeFinder, IndexBenefitGraphConstructor ibgCons, IndexBitSet bitset_Y) {
		boolean retval = true; // set false on first failure
		for (int a = candidatesBitSet.nextSetBit(0); a >= 0; a = candidatesBitSet.nextSetBit(a+1)) {
			IBGNode Y;
			double costY;
			
			// Y is just the current node
			Y = startingNode;
			costY = Y.cost();
			
			// fetch YaSimple
			bitset_YaSimple.set(bitset_Y);
			bitset_YaSimple.set(a);
			IBGNode YaSimple = coveringNodeFinder.findFast(ibgCons.rootNode(), bitset_YaSimple, null);
			if (YaSimple == null)
				retval = false;
			else
				logger.assignBenefit(a, costY - YaSimple.cost());
			
			for (int b = candidatesBitSet.nextSetBit(a+1); b >= 0; b = candidatesBitSet.nextSetBit(b+1)) {
				IBGNode Ya, Yab, YbPlus, YbMinus;
				double costYa, costYab;

				// fetch Ya and Yab
				bitset_Ya.set(bitset_Y);
				bitset_Ya.set(a);
				bitset_Ya.clear(b);
				
				bitset_Yab.set(bitset_Y);
				bitset_Yab.set(a);
				bitset_Yab.set(b);

				Yab = coveringNodeFinder.findFast(ibgCons.rootNode(), bitset_Yab, YaSimple);
				Ya = coveringNodeFinder.findFast(ibgCons.rootNode(), bitset_Ya, Yab);

				if (Ya == null) {
					retval = false;
					continue;
				}
				if (Yab == null) {
					retval = false;
					continue;
				}
				costYa = Ya.cost();
				costYab = Yab.cost();
				
				// fetch YbMinus and YbPlus
				bitset_YbMinus.clear();
				Y.addUsedIndexes(bitset_YbMinus);
				Ya.addUsedIndexes(bitset_YbMinus);
				Yab.addUsedIndexes(bitset_YbMinus);
				bitset_YbMinus.clear(a);
				bitset_YbMinus.set(b);
				
				bitset_YbPlus.set(bitset_Y);
				bitset_YbPlus.clear(a);
				bitset_YbPlus.set(b);

				YbPlus = coveringNodeFinder.findFast(ibgCons.rootNode(), bitset_YbPlus, Yab);
				YbMinus = coveringNodeFinder.findFast(ibgCons.rootNode(), bitset_YbMinus, YbPlus);
				
				// try to set lower bound based on Y, Ya, YbPlus, and Yab
				if (YbPlus != null) {
					logger.assignInteraction(a, b, interactionLevel(costY, costYa, YbPlus.cost(), costYab));
				}
				else {
					retval = false;
				}

				// try to set lower bound based on Y, Ya, YbMinus, and Yab
				if (YbMinus != null) {
					logger.assignInteraction(a, b, interactionLevel(costY, costYa, YbMinus.cost(), costYab));
				}
				else {
					retval = false;
				}
			}
		}
		
		return retval;        
    }    

    public void updateUsedSet(){
		// get the used set
		usedBitSet.clear();
		startingNode.addUsedIndexes(usedBitSet);
    }

    @Override
    public String toString() {
        return new ToStringBuilder<IBGAnalyzerWorkspace>(this)
               .add("startingNode", startingNode)
               .add("logger", logger)
               .add("candidatesBitSet", candidatesBitSet)
               .add("allUsedIndexes", allUsedIndexes)
               .add("rootBitSet", rootBitSet)
               .add("usedBitSet", usedBitSet)
               .add("bitset_YaSimple", bitset_YaSimple)
               .add("bitset_Ya", bitset_Ya)
               .add("bitset_YbMinus", bitset_YbMinus)
               .add("bitset_YbPlus", bitset_YbPlus)
               .add("bitset_Yab", bitset_Yab)
               .toString();
    }
}
