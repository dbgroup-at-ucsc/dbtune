/*
 * ****************************************************************************
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
 *  ****************************************************************************
 */

package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.SQLCategory;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.ToStringBuilder;

/**
 *  implements a DB2-specific {@link edu.ucsc.dbtune.optimizer.ExplainInfo}.
 */
public class DB2ExplainInfo extends AbstractExplainInfo {
	private final Table       updatedTable;
	private final double      updateCost;
    private final double      totalCost;

    /**
     * construct a {@code DB2ExplainInfo} object.
     * @param cat
     *      sql category
     * @param updTable
     *      updated table
     * @param updCost
     *      the updating cost.
     */
	public DB2ExplainInfo(SQLCategory cat, Table updTable, double updCost) {
        this(cat, updTable, updCost, 0.0);
	}

    public DB2ExplainInfo(SQLCategory category, Table updatedTable, double updatingCost, double totalCost){
        super(category);
        if (SQLCategory.DML.isSame(category)) {
            Checks.checkAssertion(updatedTable != null, "need updated table for DML");
            Checks.checkAssertion(updatingCost >= 0, "invalid update cost");
        }
        this.updatedTable   = (Table) updatedTable;
        this.updateCost     = updatingCost;
        this.totalCost      = totalCost;
    }

    @Override
    public double getTotalCost() {
        return totalCost;
    }

    @Override
	public double getIndexMaintenanceCost(Index index) {
		if (!SQLCategory.DML.isSame(getSQLCategory()))
			return 0;
		if (!index.getTable().equals(updatedTable))
			return 0;
		return updateCost;
	}

    @Override
    public String toString() {
        return new ToStringBuilder<DB2ExplainInfo>(this)
               .add("update table", updatedTable)
               .add("sql category", getSQLCategory())
               .add("update cost", updateCost)
               .add("isDML", isDML())
               .add("isQuery", isQuery())
               .toString();
    }
}
