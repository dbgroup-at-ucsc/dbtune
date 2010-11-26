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
import edu.ucsc.dbtune.util.ToStringBuilder;
import edu.ucsc.dbtune.core.SQLStatement.SQLCategory;

import java.io.Serializable;
import java.util.Arrays;

/**
 *  implements a PG-specific {@link edu.ucsc.satuning.db.ExplainInfo}.
 */
public class PGExplainInfo extends AbstractExplainInfo<PGSystem> implements Serializable {
	private static final long serialVersionUID = 1L;

	private final double[] updateCost;

    /**
     * construct a new {@code PGExplainInfo} object.
     * @param cat
     *      sql category.
     * @param overhead
     *      an array of incurred overheads.
     */
	public PGExplainInfo(SQLCategory cat, double[] overhead) {
        super(cat);
		updateCost = overhead;
	}

    @Override
	public double maintenanceCost(DBIndex<PGSystem> index) {
		if (getSQLCategory() != SQLCategory.DML)
			return 0;
		return updateCost[index.internalId()];
	}

    @Override
    public String toString() {
        return new ToStringBuilder<PGExplainInfo>(this)
               .add("sql category", getSQLCategory())
               .add("update cost", Arrays.toString(updateCost))
               .add("isDML", isDML())
               .toString();
    }
}
