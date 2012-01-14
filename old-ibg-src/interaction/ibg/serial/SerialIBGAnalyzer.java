package interaction.ibg.serial;

import interaction.Configuration;
import interaction.ibg.log.InteractionLogger;
import interaction.ibg.serial.SerialIndexBenefitGraph.IBGNode;
import interaction.util.BitSet;

public class SerialIBGAnalyzer {
	// the IBG we are currently exploring
	protected final SerialIndexBenefitGraph ibg;
	
	// queue of nodes to explore
	private final SerialIBGNodeQueue nodeQueue;
	
	// graph traversal objects
	private SerialIBGCoveringNodeFinder coveringNodeFinder = new SerialIBGCoveringNodeFinder();
	
	// copy of the root bit set for convenience
	private final BitSet rootBitSet;
	
	// copy of the used set for convenience
	private final BitSet allUsedIndexes;
	
	// keeps track of visited nodes
	private final BitSet visitedNodes;
	
	/*
	 * Constructor
	 */
	public SerialIBGAnalyzer(SerialIndexBenefitGraph ibg0) {
		// initialize fields 
		ibg = ibg0;
		nodeQueue = new SerialIBGNodeQueue();
		rootBitSet = ibg0.rootNode().config.clone();
		visitedNodes = new BitSet();
		allUsedIndexes = new BitSet();
		
		// seed the queue with the root node
		nodeQueue.addNode(ibg0.rootNode());
	}
	
	/*
	 * Perform analysis
	 */
	public final void doAnalysis(InteractionLogger logger) {
		// we might need to go through several nodes to find one that we haven't visited yet
		while (nodeQueue.hasNext()) {
			IBGNode node = nodeQueue.next();
			
			if (visitedNodes.get(node.id))
				continue;
			
			visitedNodes.set(node.id);
			analyzeNode(node, logger);
			nodeQueue.addChildren(node.firstChild());
		}
	}

	// We have a bunch of structures that we keep around to
	// avoid excessive garbage collection. These structures are only used in 
	// analyzeNode(). 
	// 
	// TODO: We might want to group these together in some object ("IBGAnalyzerWorkspace"?)
	private BitSet candidatesBitSet = new BitSet();
	private BitSet usedBitSet = new BitSet();

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
			for (int b = candidatesBitSet.nextSetBit(a+1); b >= 0; b = candidatesBitSet.nextSetBit(b+1)) {
				IBGNode Y, Ya, Yab, YbPlus, YbMinus;
				double costY, costYa, costYab;

				// Y is just the current node
				Y = node;
				costY = Y.cost();
				
				// fetch Ya and Yab
				bitset_Ya.set(bitset_Y);
				bitset_Ya.set(a);
				bitset_Ya.clear(b);
				
				bitset_Yab.set(bitset_Y);
				bitset_Yab.set(a);
				bitset_Yab.set(b);

				coveringNodeFinder.find(ibg, bitsetBufA, 2, nodeBuf);
				Ya = nodeBuf[0];
				Yab = nodeBuf[1];
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
				
				coveringNodeFinder.find(ibg, bitsetBufB, 2, nodeBuf);
				YbMinus = nodeBuf[0];
				YbPlus = nodeBuf[1];
				
				// set lower bound based on Y, Ya, YbPlus, and Yab
				logger.assignInteraction(a, b, interactionLevel(costY, costYa, YbPlus.cost(), costYab));

				// set lower bound based on Y, Ya, YbMinus, and Yab
				logger.assignInteraction(a, b, interactionLevel(costY, costYa, YbMinus.cost(), costYab));
			}
		}
		
		return retval;
	}

	/*
	 * Compute the interaction level based on the four costs

	 *     | C - C_a - C_b + C_ab |
	 *    --------------------------
	 *             C_ab
	 */
	private static final double interactionLevel(double empty, double a, double b, double ab) {
		return Math.abs(empty - a - b + ab) / ab;
	}
}

