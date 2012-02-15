package edu.ucsc.dbtune.seq.def;

public class SeqStep {
	public SeqQuery query;
	public SeqStepConf[] configurations;

	public SeqStep(SeqQuery query, SeqIndex[] indices) {
		this.query = query;
		this.configurations = new SeqStepConf[1];
		this.configurations[0] = new SeqStepConf(this, indices);
	}

	public SeqStep(SeqQuery query, SeqConfiguration[] cs) {
		this.query = query;
		this.configurations = new SeqStepConf[cs.length];
		for (int i = 0; i < cs.length; i++)
			this.configurations[i] = new SeqStepConf(this, cs[i]);
	}
}
