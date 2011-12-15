package edu.ucsc.dbtune.advisor.bc;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * bc.
 *
 */
public class BcIndexPool
{
    Map<Integer, BcIndexInfo> map;
    Configuration conf;

    /**
     * construct a {@code BcIndexPool} object from a hot set of indexes.
     * @param hotSet
     *      a hot set of indexes.
     */
    public BcIndexPool(Configuration conf, Configuration hotSet)
    {
        map = new HashMap<Integer, BcIndexInfo>(hotSet.size());
        for (Index idx : hotSet) {
            map.put(conf.getOrdinalPosition(idx), new BcIndexInfo());
        }
    }

    /**
     * Returns the {@code BcIndexInfo} matching an index's id.
     * @param id
     *      index's id.
     * @return the {@code BcIndexInfo} matching an index's id.
     */
    public BcIndexInfo get(int id)
    {
        return map.get(id);
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder<BcIndexPool>(this)
               .add("idToBcIndexInfo map", map)
               .toString();
    }
}
