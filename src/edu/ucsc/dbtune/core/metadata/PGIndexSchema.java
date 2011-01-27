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

import edu.ucsc.dbtune.core.DatabaseColumn;
import edu.ucsc.dbtune.core.DatabaseIndexSchema;
import edu.ucsc.dbtune.core.DatabaseTable;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.io.Serializable;
import java.util.List;

/**
 * A PG-specific implementation of {@link DatabaseIndexSchema}.
 */
public class PGIndexSchema implements DatabaseIndexSchema, Serializable {
	// serialization support
	private static final long serialVersionUID = 1L;

	private PGTable baseTable;
	private List<DatabaseColumn> columns;
	private boolean isSync;
	private List<Boolean> isDescending;
	private String signature;

    /**
     * construct a {@code PGIndexSchema} object.
     * @param reloid
     *     relation object id
     * @param isSync
     *    indicate whether is sync or not.
     * @param columns
     *      a list of {@link DatabaseColumn columns}.
     * @param isDescending
     *      indicate whether is in descending order.
     */
    public PGIndexSchema(int reloid, boolean isSync,
                         List<DatabaseColumn> columns,
                         List<Boolean> isDescending
    ) {
		this.baseTable = new PGTable(reloid);
		this.isSync = isSync;
		this.columns = columns;
		this.isDescending = isDescending;

        Checks.checkArgument(columns.size() == isDescending.size());

		final StringBuilder sb = new StringBuilder();
		sb.append(reloid);
		sb.append(isSync() ? 'Y' : 'N');
		for (DatabaseColumn col : columns) {
            final PGColumn each = Objects.as(col);
            sb.append(each.getAttnum()).append(" ");
        }

        for (boolean d : isDescending) {
            sb.append(d ? 'y' : 'n');
        }

		this.signature = sb.toString();
	}

    /**
     * @return a {@link edu.ucsc.dbtune.core.metadata.PGIndexSchema} duplicate.
     */
	public PGIndexSchema consDuplicate() {
        final PGTable table = Objects.as(getBaseTable());
		return new PGIndexSchema(table.getOid(), isSync(), getColumns(), getDescending());
	}

    @Override
	public boolean equals(Object that) {
		if (!(that instanceof PGIndexSchema))
			return false;
		final PGIndexSchema other = (PGIndexSchema) that;
		return getSignature().equals(other.getSignature());
	}

    @Override
    public DatabaseTable getBaseTable() {
        return baseTable;
    }

    @Override
    public List<DatabaseColumn> getColumns() {
        return columns;
    }

    /**
     * @return a list of {@link Boolean} that indicate whether a {@link edu.ucsc.satuning.db.DBIndex}
     *      is listed in descending or ascending order.
     */
    List<Boolean> getDescending() {
        return isDescending;
    }

    /**
     * @return
     *      the signature of {@link edu.ucsc.satuning.db.DBIndex}
     */
    String getSignature() {
        return signature;
    }

    @Override
	public int hashCode() {
		return getSignature().hashCode();
	}

    /**
     * @return {@code true} if the index is sync; {@code false} otherwise.
     */
    boolean isSync() {
        return isSync;
    }


    @Override
    public String toString() {
        return new ToStringBuilder<PGIndexSchema>(this)
               .add("baseTable", getBaseTable())
               .add("columns", getColumns())
               .add("isSync", isSync())
               .add("isDescending", getDescending())
               .add("signature", getSignature())
               .toString();
    }
}
