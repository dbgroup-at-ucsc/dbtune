package satuning.ibg;

import satuning.ibg.IndexBenefitGraph.IBGNode;
import satuning.ibg.log.InteractionLogger;
import satuning.util.BitSet;

//import interaction.util.Debug;

public class IBGAnalyzer {
	// the IBG we are currently exploring
	protected final IndexBenefitGraphConstructor ibgCons;
	
	// queue of nodes to explore
	private final IBGNodeQueue nodeQueue;
	
	// queue of nodes to revisit
	private final IBGNodeQueue revisitQueue;
	
	// the set of all used indexes seen so far in the IBG
	// this is different from the bank, because that may have 
	// used indexes from other IBG traversals
	private final BitSet allUsedIndexes;
	
	// graph traversal objects
	private IBGCoveringNodeFinder coveringNodeFinder = new IBGCoveringNodeFinder();
	
	// copy of the root bit set for convenience
	private final BitSet rootBitSet;
	
	// keeps track of visited nodes
	private final BitSet visitedNodes;
	
	/*
	 * Constructor
	 */
	public IBGAnalyzer(IndexBenefitGraphConstructor ibgCons0) {
		// initialize fields 
		ibgCons = ibgCons0;
		nodeQueue = new IBGNodeQueue();
		revisitQueue = new IBGNodeQueue();
		allUsedIndexes = new BitSet();
		rootBitSet = ibgCons0.rootNode().config.clone();
		visitedNodes = new BitSet();
		
		// seed the queue with the root node
		nodeQueue.addNode(ibgCons0.rootNode());
	}
	
	/*
	 * Perform one step of analysis
	 * 
	 * Return true if there might be some work left to do for this analysis
	 */
	public enum StepStatus { 
		BLOCKED, // there was no expanded node to analyze, and there is an unexpanded node
		DONE, // all nodes are analyzed
		SUCCESS // there was an expanded node to analyze
	}
	public final StepStatus analysisStep(InteractionLogger logger, boolean wait) {
		// we might need to go through several nodes to find one that we haven't visited yet
		while (true) {
			IBGNode node;
			
			if (nodeQueue.hasNext()) {
				if (nodeQueue.peek().isExpanded()) { 
					node = nodeQueue.next();
				}
				else if (wait) {
					node = nodeQueue.next();
					ibgCons.waitUntilExpanded(node);
				}
				else if (revisitQueue.hasNext()) {
					node = revisitQueue.next();
				}
				else {
					return StepStatus.BLOCKED;
				}
			}
			else if (revisitQueue.hasNext()) {
				node = revisitQueue.next();
			}
			else { 
				return StepStatus.DONE;
			}
			
			if (visitedNodes.get(node.id))
				continue;
			
			if (analyzeNode(node, logger)) {
				// node is done ... move on to children
//				Debug.println("analyzed node "+node.id+" of "+ibg.xacts.get(0).id);
				visitedNodes.set(node.id);
				nodeQueue.addChildren(node.firstChild());
			}
			else {
//				System.out.println("failed to analyze node "+node.id+" of "+ibg.xacts.get(0).id+", re-queueing");
				revisitQueue.addNode(node);
			}
			
			if (revisitQueue.hasNext() || nodeQueue.hasNext())
				return StepStatus.SUCCESS;
		}
	}

	// We have a bunch of structures that we keep around to
	// avoid excessive garbage collection. These structures are only used in 
	// analyzeNode(). 
	// 
	// TODO: We might want to group these together in some object ("IBGAnalyzerWorkspace"?)
	private BitSet candidatesBitSet = new BitSet();
	private BitSet usedBitSet = new BitSet();

	private BitSet bitset_YaSimple = new BitSet();
	private BitSet bitset_Ya = new BitSet();
	private BitSet bitset_YbMinus = new BitSet();
	private BitSet bitset_YbPlus = new BitSet();
	private BitSet bitset_Yab = new BitSet();
	
	// inputs/outputs for covering node finder
	private BitSet[] bitsetBufA = new BitSet[] { bitset_Ya, bitset_Yab };
	private BitSet[] bitsetBufB = new BitSet[] { bitset_YbMinus, bitset_YbPlus };
	private IBGNode[] nodeBuf = new IBGNode[]  { null, null};
	
	/*
	 * Return true if the analysis was successful
	 */
	private final boolean analyzeNode(IBGNode node, InteractionLogger logger) {		
		BitSet bitset_Y = node.config;
		
		// get the used set
		usedBitSet.clear();
		node.addUsedIndexes(usedBitSet);
		
		// store the used set
		allUsedIndexes.or(usedBitSet);
		
		// set up candidates 
		candidatesBitSet.set(rootBitSet);
		candidatesBitSet.andNot(usedBitSet);
		candidatesBitSet.and(allUsedIndexes);

		boolean retval = true; // set false on first failure
		for (int a = candidatesBitSet.nextSetBit(0); a >= 0; a = candidatesBitSet.nextSetBit(a+1)) {
			IBGNode Y;
			double costY;
			
			// Y is just the current node
			Y = node;
			costY = Y.cost();
			
			// fetch YaSimple
			bitset_YaSimple.set(bitset_Y);
			bitset_YaSimple.set(a);
			IBGNode YaSimple = coveringNodeFinder.find(ibgCons.rootNode(), bitset_YaSimple);
			if (YaSimple == null)
				retval = false;
			else
				logger.assignBenefit(a, costY - YaSimple.cost());
			
			for (int b = candidatesBitSet.nextSetBit(a+1); b >= 0; b = candidatesBitSet.nextSetBit(b+1)) {
				IBGNode Ya, Yab, YbPlus, YbMinus;
				double costYa, costYab;

//				if (bank.interactionExists(a,b)) // XXX: try to see how strong the interaction can get?? This might not affect performance a lot
//					continue;

				
				// fetch Ya and Yab
				bitset_Ya.set(bitset_Y);
				bitset_Ya.set(a);
				bitset_Ya.clear(b);
				
				bitset_Yab.set(bitset_Y);
				bitset_Yab.set(a);
				bitset_Yab.set(b);

				coveringNodeFinder.find(ibgCons.rootNode(), bitsetBufA, 2, nodeBuf);
				Ya = nodeBuf[0];
				Yab = nodeBuf[1];
				if (Ya == null) {
//					System.err.println("Missing Ya");
					retval = false;
					continue;
				}
				if (Yab == null) {
//					System.err.println("Missing Yab");
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
				
				coveringNodeFinder.find(ibgCons.rootNode(), bitsetBufB, 2, nodeBuf);
				YbMinus = nodeBuf[0];
				YbPlus = nodeBuf[1];
				
				// try to set lower bound based on Y, Ya, YbPlus, and Yab
				if (YbPlus != null) {
					logger.assignInteraction(a, b, interactionLevel(costY, costYa, YbPlus.cost(), costYab));
				}
				else {
//					System.err.println("Missing YbPlus");
					retval = false;
				}

				// try to set lower bound based on Y, Ya, YbMinus, and Yab
				if (YbMinus != null) {
					logger.assignInteraction(a, b, interactionLevel(costY, costYa, YbMinus.cost(), costYab));
				}
				else {
//					System.err.println("Missing YbMinus");
					retval = false;
				}
			}
		}
		
		return retval;
	}

	/*
	 * Compute the interaction level based on the four costs
	 *
	 *     | C - C_a - C_b + C_ab |
	 */
	private static final double interactionLevel(double empty, double a, double b, double ab) {
		return Math.abs(empty - a - b + ab);
	}
}

