package edu.ucsc.dbtune.optimizer.plan;

import java.sql.SQLException;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * An implementation of an INUM template that is given a slot cache on which to search for 
 * previously computed operator instantiations. This reduces significantly the time it takes to 
 * instantiate an INUM template.
 *
 * @author Ivo Jimenez
 */
public class InumPlanWithCache extends InumPlan
{
    private Map<String, Operator> slotCache;

    /**
     * Constructor.
     *
     * @param template
     *      template for which a cache should be activated
     * @param slotCache
     *      global cache being used
     */
    public InumPlanWithCache(InumPlan template, Map<String, Operator> slotCache)
    {
        super(template);

        this.slotCache = slotCache;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Operator instantiateOperatorForUnseenIndex(SQLStatement sql, Index index)
        throws SQLException
    {
        String key = index + sql.getSQL();

        Operator cachedOperator = slotCache.get(key);

        if (cachedOperator == null) {

            cachedOperator = super.instantiateOperatorForUnseenIndex(sql, index);

            slotCache.put(key, cachedOperator);
        }

        return cachedOperator;
    }
}
