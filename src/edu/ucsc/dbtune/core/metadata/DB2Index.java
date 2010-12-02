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
import edu.ucsc.dbtune.util.PreConditions;

import java.io.Serializable;
import java.sql.SQLException;

import static edu.ucsc.dbtune.util.PreConditions.checkNotNull;

public class DB2Index extends AbstractDatabaseIndex<DB2Index> implements Serializable {
	// serialized fields
	protected DB2IndexMetadata meta;
	private int hashCodeCache;

	// serialization support
	private static final long serialVersionUID = 1L;

    /**
     * construct a new {@code DB2Index} object.
     * @param metadata
     *      a {@link DB2IndexMetadata} object.
     * @param creationCost
     *      {@link edu.ucsc.satuning.db.DBIndex}'s creation cost.
     * @throws java.sql.SQLException
     *      unexpected error has occurred - unable to create object.
     */
    public DB2Index(DB2IndexMetadata metadata,	double creationCost) throws SQLException {
		super(
                checkNotNull(metadata).internalId,
                checkNotNull(metadata).creationText,
                creationCost,
                checkNotNull(metadata).megabytes
        );

        this.meta       = metadata;
		hashCodeCache   = metadata.hashCode();
	}

    /**
     * @return the name of table
     *      given by the index schema.
     */
	public String tableName() {
		return meta.schema.tableName;
	}

    /**
     * @return
     *      the name of the index schema which created
     *      the index.
     */
	public String tableSchemaName() {
		return meta.schema.tableCreatorName;
	}

    @Override
	public DB2QualifiedName baseTable() {
		return meta.schema.getBaseTable();
	}

	@Override
	public int columnCount() {
		return meta.schema.getColumns().size();
	}

    @Override
	public DB2Index consDuplicate(int id) throws SQLException {
		final DB2IndexMetadata dupMeta = DB2IndexMetadata.consDuplicate(meta, id);
        return new DB2Index(dupMeta, creationCost());
	}

	// crucial override
	@Override
	public boolean equals(Object obj) {
        return obj instanceof DB2Index
               && ((DB2Index) obj).meta.equals(meta);
    }

    @Override
	public DB2IndexColumn getColumn(int i) {
		return Objects.as(meta.schema.getColumns().get(i));
	}
	
	// crucial override
	@Override
	public int hashCode() {
		return hashCodeCache;
	}

    /**
     * compares qualified names.
     * @param name
     *    qualied name to be used in the comparison.
     * @return
     *      {@code true} if both qualified names are the same, false
     *      otherwise.
     */
	public boolean isOn(DB2QualifiedName name) {
		return name.equals(meta.schema.getBaseTable());
	}

    @Override
	public double megabytes() {
        // check Effective Java if you want to know why Double#compare(...)
        // is used when comparing double values.
        final boolean isPositive = Double.compare(super.megabytes(), 0.0) >= 0; 
        PreConditions.checkArgument(isPositive, "Index size is not known");
		return super.megabytes();
	}
	
	@Override
	public String toString() {
		return meta.creationText;
	}
}
