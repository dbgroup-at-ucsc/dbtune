package edu.ucsc.dbtune.seq.def;

public class SeqStep {
	public SeqQuerySet queries;
	public SeqStepConf[] configurations;

	public SeqStep(SeqQuerySet queries, SeqIndex[] indices) {
		this.queries = queries;
		this.configurations = new SeqStepConf[1];
		this.configurations[0] = new SeqStepConf(this, indices);
	}

	public SeqStep(SeqQuerySet queries, SeqConfiguration[] cs) {
		this.queries = queries;
		this.configurations = new SeqStepConf[cs.length];
		for (int i = 0; i < cs.length; i++)
			this.configurations[i] = new SeqStepConf(this, cs[i]);
	}
}
