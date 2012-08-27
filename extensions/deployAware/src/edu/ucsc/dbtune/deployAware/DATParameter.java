package edu.ucsc.dbtune.deployAware;

import ilog.concert.IloException;

import java.util.logging.Logger;

import edu.ucsc.dbtune.seq.bip.SeqInumCost;

public class DATParameter {
    public SeqInumCost costModel;
    public double[] windowConstraints;
    public double spaceConstraint;
    public double alpha;
    public double beta;
    public int totalQueires;
    public int totalIndices;
    public int maxIndexCreatedPerWindow = 0;
    public double intermediateConstraint = 0;
    Logger log = Logger.getLogger(DAT.class.getName());

    public DATParameter() {
    }

    public DATParameter(SeqInumCost cost, double[] windowConstraints,
            double alpha, double beta, int maxIndexCreatedPerWindow)
            throws IloException {
        this.costModel = cost;
        this.windowConstraints = windowConstraints;
        this.spaceConstraint = cost.storageConstraint;
        this.alpha = alpha;
        this.beta = beta;
        this.totalQueires = cost.queries.size();
        this.totalIndices = cost.indices.size();
        this.maxIndexCreatedPerWindow = maxIndexCreatedPerWindow;
    }
}
