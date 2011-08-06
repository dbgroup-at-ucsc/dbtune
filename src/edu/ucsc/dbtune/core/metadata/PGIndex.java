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

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PGIndex extends AbstractIndex implements Serializable {
    private PGIndexSchema schema;

    // serialization support
    private static final long serialVersionUID = 1L;

    /**
     * construct a new {@code PGIndex} object.
     *
     * @param reloid
     *     relation object id of the corresponding table
     * @param isSync
     *    indicate whether is sync or not.
     * @param columns
     *      a list of {@link Column columns}.
     * @param isDescending
     *      indicate whether is in descending order.
     * @param internalId
     *     {@link edu.ucsc.dbtune.core.DBIndex}'s internalId.
     * @param creationCost
     *     {@link edu.ucsc.dbtune.core.DBIndex}'s creation cost.
     * @param megabytes
     *     {@link edu.ucsc.dbtune.core.DBIndex}'s size (in megabytes).
     * @param creationText
     *     {@link edu.ucsc.dbtune.core.DBIndex}'s creation text.
     */
    public PGIndex(
            int reloid,
            boolean isSync,
            List<Column> columns,
            List<Boolean> isDescending,
            int internalId,
            double creationCost,
            double megabytes,
            String creationText
    ) {
        super(internalId, creationText, creationCost, megabytes);
        this.schema = new PGIndexSchema(reloid, isSync, columns, isDescending);
    }

    public PGIndex(
            int internalId,
            double creationCost,
            double megabytes,
            String creationText
    ) {
        super(internalId, creationText, creationCost, megabytes);
    }
    @Override
    public Table baseTable() {
        return getSchema().getBaseTable();
    }

    public List<Column> getColumns() {
        return getSchema().getColumns();
    }

    @Override
    public int columnCount() {
        return getSchema().getColumns().size();
    }

    @Override
    public PGIndex consDuplicate(int id) throws SQLException {
        return new PGIndex(
                getSchema().getRelOID(),
                getSchema().isSync(),
                getSchema().getColumns(),
                getSchema().isDescending,
                id, 
                creationCost(), 
                megabytes(), 
                creationText()
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PGIndex)){
            return false;
        }

        PGIndex other = (PGIndex) obj;
        final boolean isSchemaNull = getSchema() == null;
        return isSchemaNull ? internalId() == other.internalId()
               : getSchema().equals(other.getSchema()) && internalId() == other.internalId();
    }

    @Override
    public Column getColumn(int i) {
        return Objects.as(getSchema().getColumns().get(i));
    }

    /**
     * @return a {@link PGIndexSchema} object related in some sort to this index.
     */
    public PGIndexSchema getSchema() {
        return schema;
    }

    @Override
    public int hashCode() {
        return (Objects.hashCode(getSchema(), internalId()));
    }

    @Override
    public String toString() {
        return creationText();
    }

    public static List<PGIndex> cast(Iterable<? extends DBIndex> indexes) {
        List<PGIndex> pgIndexes = new ArrayList<PGIndex>();

        for(DBIndex each : indexes){
            pgIndexes.add((PGIndex) each);
        }

        return pgIndexes;
    }

    /**
     * A PG-specific implementation of {@link DatabaseIndexSchema}.
     */
    public static class PGIndexSchema implements Serializable {
        // serialization support
        private static final long serialVersionUID = 1L;

        private Table baseTable;
        private List<Column> columns;
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
         *      a list of {@link Column columns}.
         * @param isDescending
         *      indicate whether is in descending order.
         */
        public PGIndexSchema(int reloid, boolean isSync,
                List<Column> columns,
                List<Boolean> isDescending
                ) {
            this.baseTable = new Table(reloid);
            this.isSync = isSync;
            this.columns = columns;
            this.isDescending = isDescending;

            Checks.checkArgument(columns.size() == isDescending.size());

            final StringBuilder sb = new StringBuilder();
            sb.append(reloid);
            sb.append(isSync() ? 'Y' : 'N');
            for (Column col : columns) {
                final Column each = Objects.as(col);
                sb.append(each.getOrdinalPosition()).append(" ");
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
            final Table table = Objects.as(getBaseTable());
            return new PGIndexSchema((int)table.getId(), isSync(), getColumns(), getDescending());
        }

        @Override
        public boolean equals(Object that) {
            if (!(that instanceof PGIndexSchema))
                return false;
            final PGIndexSchema other = (PGIndexSchema) that;
            return getSignature().equals(other.getSignature());
        }

        public Table getBaseTable() {
            return baseTable;
        }

        public List<Column> getColumns() {
            return columns;
        }

        public int getRelOID() {
            return (int)baseTable.getId();
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
}
