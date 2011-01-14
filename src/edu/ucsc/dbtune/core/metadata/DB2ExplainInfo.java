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

package edu.ucsc.dbtune.core.metadata;

import edu.ucsc.dbtune.core.AbstractExplainInfo;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseTable;
import edu.ucsc.dbtune.core.SQLStatement.SQLCategory;
import edu.ucsc.dbtune.util.Debug;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.io.Serializable;

/**
 *  implements a DB2-specific {@link edu.ucsc.dbtune.core.ExplainInfo}.
 */
public class DB2ExplainInfo extends AbstractExplainInfo implements Serializable {
	private static final long serialVersionUID = 1L;
	private final DB2QualifiedName updatedTable;
	private final double updateCost;

    /**
     * construct a {@code DB2ExplainInfo} object.
     * @param cat
     *      sql category
     * @param updTable
     *      updated table
     * @param updCost
     *      the updating cost.
     */
	public DB2ExplainInfo(SQLCategory cat, DatabaseTable updTable, double updCost) {
        super(cat);
		if (SQLCategory.DML.isSame(cat)) {
			Debug.assertion(updTable != null, "need updated table for DML");
			Debug.assertion(updCost >= 0, "invalid update cost");
		}
		updatedTable = (DB2QualifiedName) updTable;
		updateCost = updCost;
	}

    @Override
	public double maintenanceCost(DBIndex index) {
		if (!SQLCategory.DML.isSame(getSQLCategory()))
			return 0;
		if (!index.isOn(updatedTable))
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
               .toString();
    }
}
