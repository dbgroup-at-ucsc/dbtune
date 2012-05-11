package edu.ucsc.dbtune.deployAware;

import java.util.Arrays;
import java.util.Comparator;

import edu.ucsc.dbtune.seq.utils.Rt;

public class MKPGreedy {
	static class Item {
		static Comparator<Item> comparator = new Comparator<Item>() {
			@Override
			public int compare(Item o1, Item o2) {
				if (o1.pw > o2.pw)
					return -1;
				if (o1.pw < o2.pw)
					return 1;
				return 0;
			}
		};
		static Comparator<Item> comparator2 = new Comparator<Item>() {
		    @Override
		    public int compare(Item o1, Item o2) {
		        if (o1.profit > o2.profit)
		            return -1;
		        if (o1.profit < o2.profit)
		            return 1;
		        return 0;
		    }
		};
		int index;
		double weight;
		double profit;
		double pw;
	}

	double[] bins;
	double[] binWeights;
	double[] items;
	double[] profits;
	double profit;
	int[] belongs;
	Item[] is;
	int cannotFitIn = 0;
	double cannotFitWeight=0;

	public MKPGreedy(double[] bins, double[] binWeights, double[] items,
			double[] profits,boolean useRatio) {
		this.bins = Arrays.copyOf(bins,bins.length);;
		this.binWeights = binWeights;
		this.items = items;
		this.profits = profits;
		this.belongs = new int[items.length];
		Arrays.fill(belongs, -1);
		is = new Item[items.length];
		for (int i = 0; i < is.length; i++) {
			is[i] = new Item();
			is[i].index = i;
			is[i].weight = items[i];
			is[i].profit = profits[i];
			is[i].pw = is[i].profit / is[i].weight;
		}
		Arrays.sort(is, useRatio?Item.comparator:Item.comparator2);
		for (Item item : is) {
			for (int i = 0; i <this. bins.length; i++) {
				if (item.weight <= this.bins[i]) {
					belongs[item.index] = i;
					this.bins[i] -= item.weight;
					break;
				}
			}
		}
		profit = 0;
		for (Item item : is) {
			if (belongs[item.index] >= 0)
				profit += item.profit * binWeights[belongs[item.index]];
			else {
				cannotFitIn++;
				cannotFitWeight+=item.weight;
			}
		}
	}

	public static void main(String[] args) {
		double[] bins = { 100, 100, 100 };
		double[] items = { 10, 20, 30, 70, 90 };
		double[] profits = { 10, 20, 30, 70, 90 };
		double[] binWeights = { 3, 2, 1 };
		MKPGreedy greedy = new MKPGreedy(bins, binWeights, items, profits,false);
		Rt.p(greedy.profit);
		for (int i : greedy.belongs) {
			System.out.print(i + " ");
		}
		System.out.println();
	}
}
