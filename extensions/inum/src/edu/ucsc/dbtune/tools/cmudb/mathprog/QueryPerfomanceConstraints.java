package edu.ucsc.dbtune.tools.cmudb.mathprog;

import edu.ucsc.dbtune.tools.cmudb.model.QueryDesc;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Nov 6, 2008
 * Time: 10:19:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class QueryPerfomanceConstraints {
    public List generate(Variables vars, List queryDescs, float frac) {
        List consts = new ArrayList();
        Iterator<String> queryIterator = vars.queryCosts.iterator();
        for (Iterator iterator = queryDescs.iterator(); iterator.hasNext();) {
            QueryDesc desc = (QueryDesc) iterator.next();
            String cost =  queryIterator.next();
            consts.add(cost + " <= " + (frac * desc.emptyCost));
        }

        return consts;
    }
}