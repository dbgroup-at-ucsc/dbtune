package edu.ucsc.dbtune.optimizer.plan;

import java.sql.SQLException;
import java.util.Map;

import edu.ucsc.dbtune.metadata.DatabaseObject;
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
    public Operator instantiate(TableAccessSlot slot, Index index)
        throws SQLException
    {
        SQLStatement sql = buildQueryForUnseenIndex(slot, this);
        String key = index + sql.getSQL();

        Operator cachedOperator = slotCache.get(key);

        if (cachedOperator == null) {

            cachedOperator = super.instantiate(slot, index);

            slotCache.put(key, cachedOperator);
        }
        
        Operator op;
        if ( cachedOperator!=INCOMPATIBLE) {
            // The same operator may be used twice in a plan. 
            // So we need to create a separate object.
            op=new Operator(cachedOperator);
            op.cardinalityNLJ = slot.cardinalityNLJ;
            op.coefficient=slot.coefficient;
            for (DatabaseObject o : cachedOperator.getDatabaseObjects())
                op.add(o);
        } else {
            op= cachedOperator;
        }
        return op;
    }
}
