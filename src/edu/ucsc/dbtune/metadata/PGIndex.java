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

package edu.ucsc.dbtune.metadata;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.io.Serializable;
import java.util.List;

public class PGIndex extends Index {
    private PGIndexSchema schema;
    private String creationText;

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
     *     {@link edu.ucsc.dbtune.metadata.Index}'s internalId.
     * @param creationCost
     *     {@link edu.ucsc.dbtune.metadata.Index}'s creation cost.
     * @param megabytes
     *     {@link edu.ucsc.dbtune.metadata.Index}'s size (in megabytes).
     * @param creationText
     *     {@link edu.ucsc.dbtune.metadata.Index}'s creation text.
     */
    public PGIndex(
            int reloid,
            boolean isSync,
            List<Column> columns,
            List<Boolean> isDescending,
            int internalId,
            double megabytes,
            double creationCost,
            String creationText
    ) throws Exception {
        super("", (Table) null, SECONDARY, NON_UNIQUE, UNCLUSTERED);

        this.id           = internalId;
        this.creationText = creationText;
        this.creationCost = creationCost;
        this.schema       = new PGIndexSchema(reloid, isSync, columns, isDescending);
        this.columns      = columns;
        this.size         = (long) megabytes;
        this.table        = getSchema().baseTable;
        this.descending   = getSchema().getDescending();

        if(isSync) {
            this.scanOption = SYNCHRONIZED;
        }
    }

    public PGIndex(
            int internalId,
            double creationCost,
            double megabytes,
            String creationText
    )  throws Exception {
        super("", (Table) null, SECONDARY, NON_UNIQUE, UNCLUSTERED);
        this.id           = internalId;
        this.creationText = creationText;
        this.creationCost = creationCost;
        this.size         = (long) megabytes;
    }

    /**
     * @return a {@link PGIndexSchema} object related in some sort to this index.
     */
    public PGIndexSchema getSchema() {
        return schema;
    }

    @Override
    public String toString() {
        return "id: " + id + "; sql = " + creationText;
    }

    @Override
    public String getCreateStatement() {
        return creationText;
    }

    /**
     * A PG-specific implementation of an schema
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
