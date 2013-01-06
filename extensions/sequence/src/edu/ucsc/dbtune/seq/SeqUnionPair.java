package edu.ucsc.dbtune.seq;

import java.sql.SQLException;
import java.util.Vector;

import edu.ucsc.dbtune.seq.def.*;
import edu.ucsc.dbtune.util.Rt;

public class SeqUnionPair {
    public SeqStep[] steps;
    public SeqStepConf[] bestPath;

    public SeqUnionPair(SeqCost cost, SeqQuerySet[] queries, SeqStepConf[] a,
            SeqStepConf[] b) throws SQLException {
        int n = queries.length;
        steps = new SeqStep[n + 2];
        steps[0] = new SeqStep(null, cost.source);
        steps[steps.length - 1] = new SeqStep(null, cost.destination);
        for (int i = 0; i < n; i++) {
            if (!a[i + 1].step.queries.equals(b[i + 1].step.queries))
                throw new Error();
            Vector<SeqConfiguration> cs = new Vector<SeqConfiguration>();
            cs.add(a[i + 1].configuration);
            cs.add(b[i + 1].configuration);
            SeqConfiguration c = a[i + 1].configuration
                    .combine(b[i + 1].configuration);
            if (c.storageCost() <= cost.storageConstraint
                    && c.indices.length <= cost.maxIndexes)
                cs.add(c);
            steps[1 + i] = new SeqStep(queries[i], cs
                    .toArray(new SeqConfiguration[cs.size()]));
        }
        SeqOptimal optimal;
        try {
            optimal = new SeqOptimal(cost, cost.source, cost.destination,
                    queries, steps);
        } catch (Error e) {
            for (int i = 0; i < steps.length; i++) {
                Rt.p(i);
                SeqStep step = steps[i];
                for (int j = 0; j < step.configurations.length; j++) {
                    Rt.p(step.configurations[j]);
                }
            }
            throw e;
        }
        bestPath = optimal.getBestSteps();
    }
}
