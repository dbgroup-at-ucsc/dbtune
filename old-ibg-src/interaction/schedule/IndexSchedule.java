package interaction.schedule;

import java.util.LinkedList;
import java.util.ListIterator;

public class IndexSchedule {
	private LinkedList<IndexScheduleItem> list;
	
	public IndexSchedule() {
		list = new LinkedList<IndexScheduleItem>();
	}
	
	public final double benefit() {
		double benefit = 0;
		for (IndexScheduleItem item : list) {
			benefit += item.conditionalBenefit;
		}
		return benefit;
	}
	
	public final double utility() {
		int n = list.size();
		return (n+1)*benefit() - penalty();
	}
	
	public final double penalty() {
		int i = 1;
		double penalty = 0;
		for (IndexScheduleItem item : list) {
			penalty += i*item.conditionalBenefit;
			++i;
		}
		return penalty - benefit();
	}
	
	public final int indexCount() {
		return list.size();
	}
	
	public final void append(int id, double conditionalBenefit) {
		assert(conditionalBenefit >= 0);
		list.addLast(new IndexScheduleItem(id, conditionalBenefit));
	}
	
	public final void prepend(int id, double conditionalBenefit) {
		assert(conditionalBenefit >= 0);
		list.addFirst(new IndexScheduleItem(id, conditionalBenefit));
	}
	
	public final Iterable<IndexScheduleItem> items() {
		return list;
	}
	
	public final ListIterator<IndexScheduleItem> itemListIterator() {
		return list.listIterator();
	}
	
	public final void print() {
		System.out.println("Schedule penalty = " + penalty());
		System.out.print("\t[ ");
		for (IndexScheduleItem item : list) {
			System.out.printf("%d(%f) ", item.id, item.conditionalBenefit);
		}
		System.out.println("]");
	}
}

class IndexScheduleItem {
	final int id;
	double conditionalBenefit;
	
	IndexScheduleItem(int id0, double ben0) {
		id = id0;
		conditionalBenefit = ben0;
	}
}