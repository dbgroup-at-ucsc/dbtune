package edu.ucsc.dbtune.deployAware;

import java.util.Vector;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;

public class DATOutput extends IndexTuningOutput {
    public Vector<SeqInumIndex>[] indexUsed;
}
