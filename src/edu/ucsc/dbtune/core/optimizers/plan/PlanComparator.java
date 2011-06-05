/* ************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.core.optimizers.plan;

import edu.ucsc.dbtune.core.metadata.Index;

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
    public static List<Index> difference(SQLStatementPlan plan1, SQLStatementPlan plan2) {
        Set<Index> set1 = new HashSet<Index>(plan1.getIndexes());

        set1.removeAll(plan2.getIndexes());

        return new ArrayList<Index>(set1);
    }
}
