package interaction.schedule;

import interaction.util.BitSet;

public interface IBGScheduleInfo<T extends IBGScheduleInfo.Graph> {
	public T[] ibgs();
	
	public Searcher<T> searcher();
	
	public interface Graph {
		public Node rootIBGInfoNode();
		public double emptyCost();
	}
	
	public interface Searcher<T extends Graph> {
		public double findCost(T ibg, BitSet config);
		public double findCost(T[] ibgs, BitSet config);
	}
	
	public interface Node {
		public double cost();
		public boolean usedSetContains(int id);
	}
}
