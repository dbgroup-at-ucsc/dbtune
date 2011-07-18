package edu.ucsc.dbtune.tools.cmudb.mathprog;

import edu.ucsc.dbtune.tools.cmudb.model.Index;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Nov 6, 2008
 * Time: 9:19:16 PM
 * To change this template use File | Settings | File Templates.
 * translates: FOR T TABLES FOR I in indexes(T) ASSERT count(I) <= 4
 */
public class IndexCountConstraint {
    // input is the list of variables for indexes.

    public List generate(Variables vars, int maxIndex) {
        Map tableConstraints = new HashMap();
        int idx = 0;
        for (Iterator<Index> iterator = vars.indexes.iterator(); iterator.hasNext();) {
            Index index = iterator.next();
            index.getTableName();
            StringBuffer buf = (StringBuffer) tableConstraints.get(index.getTableName());
            String varName = "x" + idx;
            if(buf == null) {
                buf = new StringBuffer(varName);
            } else {
                buf.append("+ " + varName);
            }
        }

        List list = new ArrayList();
        for (Iterator iter = tableConstraints.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<String,StringBuffer> entry = (Map.Entry) iter.next();
            list.add(entry.getValue()+" <= " + maxIndex);
        }
        return list;
    }
}
