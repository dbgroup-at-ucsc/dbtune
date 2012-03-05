package edu.ucsc.dbtune.seq.bip;

import java.util.Vector;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;

public class SebBIPOutput extends IndexTuningOutput {
    public Vector<SeqInumIndex>[] indexUsed;
}
