package edu.ucsc.dbtune.tools.cmudb.greedy;

import edu.ucsc.dbtune.tools.cmudb.model.Index;
import edu.ucsc.dbtune.tools.cmudb.model.QueryDesc;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: dash
 * Date: Jul 31, 2009
 * Time: 5:25:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class GreedyResult {
    public List<QueryDesc> queryDescs;
    public List<Map<String,Index>> usedIndexes;
    public float queryCosts[];
}
