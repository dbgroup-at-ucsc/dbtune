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

import edu.ucsc.dbtune.util.Objects;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * todo
 */
public class PGIndex extends AbstractDatabaseIndex<PGIndex> implements Serializable {
	private PGIndexSchema schema;

	// serialization support
	private static final long serialVersionUID = 1L;

    /**
     * construct a new {@code PGIndex} object.
     * @param schema
     *      a {@link PGIndexSchema} object.
     * @param internalId
     *     {@link edu.ucsc.satuning.db.DBIndex}'s internalId.
     * @param creationCost
     *     {@link edu.ucsc.satuning.db.DBIndex}'s creation cost.
     * @param megabytes
     *     {@link edu.ucsc.satuning.db.DBIndex}'s size (in megabytes).
     * @param creationText
     *     {@link edu.ucsc.satuning.db.DBIndex}'s creation text.
     */
    public PGIndex(
            PGIndexSchema schema,
            int internalId,
            double creationCost,
            double megabytes,
            String creationText
    ) {
		super(internalId, creationText, creationCost, megabytes);
        this.schema = schema;
	}

    @Override
	public PGTable baseTable() {
		return Objects.as(getSchema().getBaseTable());
	}

    @Override
	public int columnCount() {
		return getSchema().getColumns().size();
	}

    @Override
	public PGIndex consDuplicate(int id) throws SQLException {
		return new PGIndex(
                getSchema().consDuplicate(),
                id, 
                creationCost(), 
                megabytes(), 
                creationText()
        );
	}

    @Override
	public boolean equals(Object obj) {
		if (!(obj instanceof PGIndex))
			return false;
		PGIndex other = (PGIndex) obj;
		return getSchema().equals(other.getSchema());
	}

    @Override
	public PGIndexColumn getColumn(int i) {
		return Objects.as(getSchema().getColumns().get(i));
	}

    public PGIndexSchema getSchema() {
        return schema;
    }

    @Override
	public int hashCode() {
		return getSchema().hashCode();
	}

    @Override
	public String toString() {
		return creationText();
	}

}
