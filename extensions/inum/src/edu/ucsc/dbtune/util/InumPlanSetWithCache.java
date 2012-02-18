package edu.ucsc.dbtune.util;

import java.util.HashMap;
import java.util.HashSet;

import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.InumPlanWithCache;
import edu.ucsc.dbtune.optimizer.plan.Operator;

/**
 * @author Ivo Jimenez
 */
public class InumPlanSetWithCache extends HashSet<InumPlan>
{
    private static final long serialVersionUID = 0;
    private static final HashMap<String, Operator> SLOT_CACHE = new HashMap<String, Operator>();

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean add(InumPlan a)
    {
        return super.add(new InumPlanWithCache(a, SLOT_CACHE));
    }
}
