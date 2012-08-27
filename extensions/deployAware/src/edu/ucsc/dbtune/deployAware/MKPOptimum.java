package edu.ucsc.dbtune.deployAware;

import java.util.Arrays;

import edu.ucsc.dbtune.util.Rt;

public class MKPOptimum {
	double maxProfit = 0;
	int[] maxBelongs;
	double[] bins;
	double[] binWeights;
	double[] items;
	double[] profits;
	int[] belongs;
	int cannotFitIn = 0;

	public MKPOptimum(double[] bins, double[] binWeights, double[] items,
			double[] profits) {
		this.bins = bins;
		this.binWeights = binWeights;
		this.items = items;
		this.profits = profits;
		this.belongs = new int[items.length];
		Arrays.fill(belongs, -1);
		solve(0, 0);
		cannotFitIn = 0;
		for (int i : maxBelongs) {
			if (i < 0)
				cannotFitIn++;
		}
	}

	public void solve(int pos, double profit) {
		if (pos == items.length) {
			if (profit > maxProfit) {
				maxProfit = profit;
				maxBelongs = Arrays.copyOf(belongs, belongs.length);
			}
			return;
		}
		for (belongs[pos] = -1; belongs[pos] < bins.length; belongs[pos]++) {
			int b = belongs[pos];
			if (b == -1) {
				solve(pos + 1, profit);
			} else {
				if (bins[b] < items[pos])
					continue;
				bins[b] -= items[pos];
				solve(pos + 1, profit + profits[pos] * binWeights[b]);
				bins[b] += items[pos];
			}
		}
	}

	public static void main(String[] args) {
		double[] bins = { 100, 100, 100 };
		double[] items = { 10, 20, 30, 70, 90 };
		double[] profits = { 10, 20, 30, 70, 90 };
		double[] binWeights = { 3, 2, 1 };
		MKPOptimum optimum = new MKPOptimum(bins, binWeights, items, profits);
		Rt.p(optimum.maxProfit);
		for (int i : optimum.maxBelongs) {
			System.out.print(i + " ");
		}
		System.out.println();
	}
}
