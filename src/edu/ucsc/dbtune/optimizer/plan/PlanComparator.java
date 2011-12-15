package edu.ucsc.dbtune.optimizer.plan;

import edu.ucsc.dbtune.metadata.Index;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class to ease the comparison of plans.
 */
public class PlanComparator
{
    /**
     * Returns the indexes that are in {@code plan1} but not in {@code plan2}.
     *
     * @param plan1
     *     plan whose indexes are compared against {@code plan2}
     * @param plan2
     *     plan whose indexes are compared against {@code plan1}
     * @return
     *     list of {@code Index} objects that are in {@code plan1} but not in {@code plan2}.
     */
    public static List<Index> difference(SQLStatementPlan plan1, SQLStatementPlan plan2)
    {
        Set<Index> set1 = new HashSet<Index>(plan1.getIndexes());

        set1.removeAll(plan2.getIndexes());

        return new ArrayList<Index>(set1);
    }
}
