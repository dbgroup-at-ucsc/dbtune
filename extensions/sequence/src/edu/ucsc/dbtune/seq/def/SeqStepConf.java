package edu.ucsc.dbtune.seq.def;

public class SeqStepConf {
	public SeqStep step;
	public SeqConfiguration configuration;
	public double costUtilThisStepBoost = 0;
	public double costUtilThisStep = 0;
	public SeqStepConf bestPreviousConfiguration;
	public double transitionCost = 0;
	public double queryCost = 0;
	public boolean isBestPath=false;

	public SeqStepConf(SeqStep step, SeqIndex[] indices) {
		this.step = step;
		this.configuration = new SeqConfiguration(indices);
	}

	public SeqStepConf(SeqStep step, SeqConfiguration configuration) {
		this.step = step;
		this.configuration = configuration;
	}

	@Override
	public String toString() {
		return "{" + configuration + "} "
				+ String.format("%.0f", costUtilThisStepBoost);
	}
}
