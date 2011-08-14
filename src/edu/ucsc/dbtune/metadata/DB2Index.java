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
import edu.ucsc.dbtune.optimizer.IBGWhatIfOptimizer;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.util.IndexSet;
import edu.ucsc.dbtune.util.HashFunction;
import edu.ucsc.dbtune.util.Instances;
import edu.ucsc.dbtune.util.Strings;

import java.io.Serializable;
import java.sql.SQLException;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.sql.ResultSet;

public class DB2Index extends Index {
    // serialized fields
    private DB2IndexMetadata meta;
    private int hashCodeCache;

    /**
     * construct new {@code DB2Index} from the ADVISE_INDEX table
     *
     */
    public DB2Index(DatabaseConnection connection, ResultSet rs, String dbName, int id, double megabytes) throws SQLException, Exception {
        super("",(Table)null,SECONDARY,NON_UNIQUE,UNCLUSTERED);

        assert connection != null;

        // construct the schema
        String tableName = rs.getString(DB2IndexMetadata.AdviseIndexColumn.TBNAME.ordinal() + 1).trim();
        String tableCreator = rs.getString(DB2IndexMetadata.AdviseIndexColumn.TBCREATOR.ordinal() + 1).trim();
        
        String colNamesString = rs.getString(DB2IndexMetadata.AdviseIndexColumn.COLNAMES.ordinal() + 1);
        List<String> colNames = new ArrayList<String>(); 
        List<Boolean> descending = new ArrayList<Boolean>();
        DB2IndexMetadata.parseColNames(colNamesString, colNames, descending);

        String uniqueRuleString = rs.getString(DB2IndexMetadata.AdviseIndexColumn.UNIQUERULE.ordinal() + 1);
        DB2IndexMetadata.DB2IndexSchema.UniqueRule uniqueRule = DB2IndexMetadata.DB2IndexSchema.UniqueRule.parse(uniqueRuleString);

        String reverseScanString = rs.getString(DB2IndexMetadata.AdviseIndexColumn.REVERSE_SCANS.ordinal() + 1);
        DB2IndexMetadata.DB2IndexSchema.ReverseScanOption reverseScan = DB2IndexMetadata.DB2IndexSchema.ReverseScanOption.parse(reverseScanString);

        String indexTypeString = rs.getString(DB2IndexMetadata.AdviseIndexColumn.INDEXTYPE.ordinal() + 1);
        DB2IndexMetadata.DB2IndexSchema.TypeOption indexType = DB2IndexMetadata.DB2IndexSchema.TypeOption.parse(indexTypeString);
        
        DB2IndexMetadata.DB2IndexSchema schema = new DB2IndexMetadata.DB2IndexSchema(dbName, tableName, tableCreator, colNames, descending,
                                                   uniqueRule, reverseScan, indexType);
        
        // construct the object
        String indexName   = DB2IndexMetadata.INDEX_NAME_BASE + id;
        String indexOwner  = rs.getString(DB2IndexMetadata.AdviseIndexColumn.EXPLAIN_REQUESTER.ordinal() + 1);
        String indexExists = rs.getString(DB2IndexMetadata.AdviseIndexColumn.EXISTS.ordinal() + 1);
        int systemRequired = rs.getInt(DB2IndexMetadata.AdviseIndexColumn.SYSTEM_REQUIRED.ordinal() + 1);
        this.setMeta(new DB2IndexMetadata(schema, id, indexName, indexOwner, indexExists, systemRequired, megabytes));
        this.id            = this.getMeta().internalId;
        this.size          = (long) this.getMeta().megabytes;
        this.creationCost  = this.getMeta().creationCost(connection.getIBGWhatIfOptimizer());
        hashCodeCache      = getMeta().hashCode();
    }

    private DB2Index(DB2IndexMetadata metadata, double creationCost) throws SQLException, Exception {
        super("",(Table)null,SECONDARY,NON_UNIQUE,UNCLUSTERED);

        this.id           = this.getMeta().internalId;
        this.size         = (long) this.getMeta().megabytes;
        this.creationCost = creationCost;
        this.setMeta(metadata);
        hashCodeCache     = metadata.hashCode();
    }

    public DB2Index(
            String dbName,
            String tableName,
            String tableCreator,
            List<String> colNames,
            List<Boolean> descending,
            String uniqueRule,
            String reverseScanOpt,
            String indexType,
            int internalId,
            String indexName,
            String indexOwner,
            String indexExists,
            int systemRequired,
            double megabytes,
            double creationCost)
        throws SQLException, Exception
    {
        super("",(Table)null,SECONDARY,NON_UNIQUE,UNCLUSTERED);

        DB2IndexMetadata.DB2IndexSchema schema = new DB2IndexMetadata.DB2IndexSchema(dbName, tableName, tableCreator, colNames, descending,
                                                   uniqueRule, reverseScanOpt, indexType);
        this.setMeta(new DB2IndexMetadata(schema, internalId, indexName, indexOwner, indexExists, systemRequired, megabytes));
        this.id           = this.getMeta().internalId;
        this.size         = (long) this.getMeta().megabytes;
        this.creationCost = creationCost;
        hashCodeCache     = getMeta().hashCode();
    }

    public static class DB2IndexSet extends IndexSet {
        private static final long serialVersionUID = IndexSet.serialVersionUID;
    }

    /**
     * @return the name of table
     *      given by the index schema.
     */
    public String tableName() {
        return getMeta().schema.tableName;
    }

    /**
     * @return
     *      the name of the index schema which created
     *      the index.
     */
    public String tableSchemaName() {
        return getMeta().schema.tableCreatorName;
    }

    @Override
    public Table getTable() {
        return getMeta().schema.getBaseTable();
    }

    @Override
    public int size() {
        return getMeta().schema.getColumns().size();
    }

    // crucial override
    @Override
    public boolean equals(Object obj) {
        return obj instanceof DB2Index
               && ((DB2Index) obj).getMeta().equals(getMeta());
    }

    @Override
    public Column getColumn(int i) {
        //noinspection RedundantTypeArguments
        return getMeta().schema.getColumns().get(i);
    }
    
    // crucial override
    @Override
    public int hashCode() {
        return hashCodeCache;
    }

    @Override
    public String toString() {
        return getMeta().creationText;
    }

    public String getCreateStatement() {
        return getMeta().creationText;
    }

    public long getMegaBytes() {
        return size;
    }

    public void setMeta(DB2IndexMetadata meta) {
        this.meta = meta;
    }

    public DB2IndexMetadata getMeta() {
        return meta;
    }

    public static class DB2IndexMetadata implements Serializable {
        // serialized fields
        protected DB2IndexSchema schema;
        protected int internalId;
        protected String indexName;
        protected String indexOwner;
        protected String indexExists;
        protected int systemRequired;
        protected String creationText;
        protected double megabytes;
        
        // for index names
        public static final String INDEX_NAME_BASE = "recommendation_tool_index_";

        // serialization support
        private static final long serialVersionUID = 1L;

        //todo(Huascar) consider making this constructor public.
        private DB2IndexMetadata(
                DB2IndexSchema schema,
                int internalId,
                String indexName,
                String indexOwner,
                String indexExists,
                int systemRequired,
                double megabytes
        ) throws SQLException {
            this.schema = schema;
            this.internalId = internalId;
            this.creationText = this.schema.creationText(indexName);
            this.indexName = indexName;
            this.indexOwner = indexOwner;
            this.indexExists = indexExists;
            this.systemRequired = systemRequired;
            this.megabytes = megabytes;
            
            if (indexExists.equals("Y"))
                throw new UnsupportedOperationException("do not handle existing indexes yet");
        }
        
        /*
         * Format the SQL that will go into the VALUES clause of an INSERT
         * in order to create a row in ADVISE_INDEX for this index.
         * 
         * Most of this is straightforward. The only option is given by the
         * 'enable' parameter. When true, this enables the index for what-if
         * optimization. When false, the optimizer will ignore the row, even
         * when in EVALUATE INDEXES mode
         */
        public void adviseIndexRowText(StringBuilder sbuf, boolean enable) {
            boolean first = true;
            sbuf.append('(');
            for (AdviseIndexColumn col : AdviseIndexColumn.values()) {
                if (!first)
                    sbuf.append(", ");
                first = false;
                
                switch (col) {
                    case EXPLAIN_REQUESTER:
                        Strings.formatStringLiteral(indexOwner, sbuf);
                        break;
                    case TBCREATOR:
                        Strings.formatStringLiteral(schema.tableCreatorName, sbuf);
                        break;
                    case TBNAME:
                        Strings.formatStringLiteral(schema.tableName, sbuf);
                        break;
                    case COLNAMES:
                        Strings.formatStringLiteral(formatColNames(schema.getColumns(), schema.descending), sbuf);
                        break;
                    case COLCOUNT:
                        sbuf.append(schema.getColumns().size());
                        break;
                    case UNIQUERULE:
                        Strings.formatStringLiteral(schema.uniqueRule.getCode(), sbuf);
                        break;
                    case UNIQUE_COLCOUNT:
                        sbuf.append(isUnique() ? schema.getColumns().size() : -1);
                        break;
                    case REVERSE_SCANS:
                        Strings.formatStringLiteral(schema.reverseScan.code, sbuf);
                        break;
                    case INDEXTYPE:
                        Strings.formatStringLiteral(schema.indexType.getCode(), sbuf);
                        break;
                    case NAME:
                        Strings.formatStringLiteral(indexName, sbuf);
                        break;
                    case CREATION_TEXT:
                        Strings.formatStringLiteral(creationText, sbuf);
                        break;
                    case EXISTS:
                        Strings.formatStringLiteral(indexExists, sbuf);
                        break;
                    case SYSTEM_REQUIRED:
                        sbuf.append(systemRequired);
                        break;
                    case IID:
                        sbuf.append(internalId);
                        break;
                    case USE_INDEX:
                        sbuf.append(enable ? "'Y'" : "'N'");
                        break;
                    default:
                        sbuf.append(col.defaultValue);
                }
            }
            sbuf.append(')');
        }

        public double creationCost(IBGWhatIfOptimizer whatIfOptimizer) throws SQLException{
            int idx             = 0;
            int nSortedColumns;
            final StringBuilder sql = new StringBuilder(16 * (nSortedColumns = schema.descending.size()));
            sql.append("SELECT ");
            for(Column each : schema.getColumns()){
                if(idx >= nSortedColumns) break;
                if(idx > 0) {
                    sql.append(',');
                }

                Strings.formatIdentifier(each.getName(), sql);
                idx++;
            }

            sql.append(" FROM ");
            Strings.formatIdentifier(schema.dbName, sql);
            sql.append('.');
            Strings.formatIdentifier(schema.tableCreatorName, sql);
            sql.append('.');
            Strings.formatIdentifier(schema.tableName, sql);

            sql.append(" ORDER BY ");
            idx = 0;
            for (Column c : schema.getColumns()) {
                if (idx > 0) {
                    sql.append(',');
                }

                Strings.formatIdentifier(c.getName(), sql);
                idx++;
            }

            return whatIfOptimizer.estimateCost(sql.toString(), Instances.newBitSet(), Instances.newBitSet());
        }
        
        public double creationCost(DatabaseConnection conn) throws SQLException {
            int i, n;
            
            StringBuilder sqlbuf = new StringBuilder();
            sqlbuf.append("SELECT ");

            i = 0; 
            n = schema.descending.size(); // n is number of *sorted* columns
            for (Column c : schema.getColumns()) {
                if (i >= n) break;
                if (i > 0) sqlbuf.append(',');
                Strings.formatIdentifier(c.getName(), sqlbuf);
                i++;
            }
            
            sqlbuf.append(" FROM ");
            Strings.formatIdentifier(schema.dbName, sqlbuf);
            sqlbuf.append('.');
            Strings.formatIdentifier(schema.tableCreatorName, sqlbuf);
            sqlbuf.append('.');
            Strings.formatIdentifier(schema.tableName, sqlbuf);
            
            sqlbuf.append(" ORDER BY ");
            i = 0;
            for (Column c : schema.getColumns()) {
                if (i > 0) sqlbuf.append(',');
                Strings.formatIdentifier(c.getName(), sqlbuf);
                i++;
            }

            return calculateTotalCost(conn, sqlbuf.toString());
        }

        private static double calculateTotalCost(DatabaseConnection connection, String sql) throws SQLException {
            return connection.getIBGWhatIfOptimizer().estimateCost(sql, Instances.newBitSet(), Instances.newBitSet());
        }
        
        public static DB2IndexMetadata consDuplicate(DB2IndexMetadata original, int id) throws SQLException {
            // construct the object
            String indexName = INDEX_NAME_BASE + id;
            String indexOwner = original.indexOwner;
            String indexExists = original.indexExists;
            int systemRequired = original.systemRequired;
            double megabytes = original.megabytes;
            
            return new DB2IndexMetadata(original.schema, id, indexName, indexOwner, indexExists, systemRequired, megabytes);
        }
        
        public static DB2IndexMetadata consSingleColumn(DB2IndexMetadata original, String col, int id, double megabytes) throws SQLException {
            // construct the schema
            String dbName = original.schema.dbName;
            String tableName = original.schema.tableName;
            String tableCreator = original.schema.tableCreatorName;
            
            List<String> colNames = new ArrayList<String>();
            colNames.add(col);
            
            List<Boolean> descending = new ArrayList<Boolean>();
            descending.add(false);
            
            DB2IndexSchema.UniqueRule uniqueRule = DB2IndexSchema.UniqueRule.NONE;
            DB2IndexSchema.ReverseScanOption reverseScan = DB2IndexSchema.ReverseScanOption.REVERSIBLE;
            DB2IndexSchema.TypeOption indexType = DB2IndexSchema.TypeOption.REGULAR;
            
            DB2IndexSchema schema = new DB2IndexSchema(dbName, tableName, tableCreator, colNames, descending,
                                                       uniqueRule, reverseScan, indexType);
            
            // construct the object
            String indexName = INDEX_NAME_BASE + id;
            String indexOwner = original.indexOwner;
            String indexExists = "N";
            int systemRequired = 0;
            
            return new DB2IndexMetadata(schema, id, indexName, indexOwner, indexExists, systemRequired, megabytes);
        }
        
        /* construct an index from the ADVISE_INDEX table */
        public static DB2IndexMetadata consFromAdviseIndex(ResultSet rs, String dbName, int id, double megabytes) throws SQLException {
            // construct the schema
            String tableName = rs.getString(AdviseIndexColumn.TBNAME.ordinal() + 1).trim();
            String tableCreator = rs.getString(AdviseIndexColumn.TBCREATOR.ordinal() + 1).trim();
            
            String colNamesString = rs.getString(AdviseIndexColumn.COLNAMES.ordinal() + 1);
            List<String> colNames = new ArrayList<String>(); 
            List<Boolean> descending = new ArrayList<Boolean>();
            parseColNames(colNamesString, colNames, descending);

            String uniqueRuleString = rs.getString(AdviseIndexColumn.UNIQUERULE.ordinal() + 1);
            DB2IndexSchema.UniqueRule uniqueRule = DB2IndexSchema.UniqueRule.parse(uniqueRuleString);

            String reverseScanString = rs.getString(AdviseIndexColumn.REVERSE_SCANS.ordinal() + 1);
            DB2IndexSchema.ReverseScanOption reverseScan = DB2IndexSchema.ReverseScanOption.parse(reverseScanString);

            String indexTypeString = rs.getString(AdviseIndexColumn.INDEXTYPE.ordinal() + 1);
            DB2IndexSchema.TypeOption indexType = DB2IndexSchema.TypeOption.parse(indexTypeString);
            
            DB2IndexSchema schema = new DB2IndexSchema(dbName, tableName, tableCreator, colNames, descending,
                                                       uniqueRule, reverseScan, indexType);
            
            // construct the object
            String indexName = INDEX_NAME_BASE + id;
            String indexOwner = rs.getString(AdviseIndexColumn.EXPLAIN_REQUESTER.ordinal() + 1);
            String indexExists = rs.getString(AdviseIndexColumn.EXISTS.ordinal() + 1);
            int systemRequired = rs.getInt(AdviseIndexColumn.SYSTEM_REQUIRED.ordinal() + 1);
            
            return new DB2IndexMetadata(schema, id, indexName, indexOwner, indexExists, systemRequired, megabytes);
        }

        @Override
        public boolean equals(Object o1) {
            return o1 instanceof DB2IndexMetadata
                   && ((DB2IndexMetadata) o1).schema.equals(schema);
        }

        private String formatColNames(List<Column> columns, List<Boolean> descending) {
            int colCount = columns.size();
            if (descending.size() != colCount)
                throw new UnsupportedOperationException("do not handle INCLUDE columns yet");
            
            StringBuilder sbuf = new StringBuilder();
            for (int i = 0; i < colCount; i++) {
                sbuf.append(descending.get(i) ? '-' : '+');
                final Column each = columns.get(i);
                sbuf.append(each.getName());
            }
            
            return sbuf.toString();
        }
        
        @Override
        public int hashCode() {
            return schema.hashCode();
        }
        
        /*
         * Returns true if the index has unique keys.
         * Both unique and primary indexes have unique keys.
         */
        public boolean isUnique() {
            switch (schema.uniqueRule) {
                case UNIQUE:
                case PRIMARY: return true;
                case NONE: 
                default: return false;
            }
        }
        
        private static void parseColNames(String str, List<String> colNames, List<Boolean> descending) 
        throws SQLException {
            char c;
            int nameStart;
            
            c = str.charAt(0);
            if (c == '+')
                descending.add(false);
            else if (c == '-')
                descending.add(true);
            else 
                throw new SQLException("first character '" + c + "' unexpected in ADVISE_INDEX.COLNAMES");
            
            nameStart = 1; // name starts after +/- symbol
            
            for (int i = 1; i < str.length(); i++) {
                boolean newColumn;
                c = str.charAt(i);
                if (c == '+') {
                    descending.add(false);
                    newColumn = true;
                }
                else if (c == '-') {
                    descending.add(true);
                    newColumn = true;
                }
                else {
                    newColumn = false;
                }
                
                if (newColumn) {
                    if (i - nameStart < 1)
                        throw new SQLException("empty column name found in ADVISE_INDEX.COLNAMES");
                    colNames.add(str.substring(nameStart, i));
                    nameStart = i + 1;
                }
            }
            
            if (str.length() - nameStart < 1)
                throw new SQLException("empty column name found in ADVISE_INDEX.COLNAMES");
            colNames.add(str.substring(nameStart, str.length()));
        }
        
        @Override
        public String toString() {
            return creationText;
        }
        
        /* ------------------
         * ADVISE_INDEX stuff
         * 
         * We assume that the table is SELECTed and INSERTed with the columns in this order
         * ------------------
         */
        public enum AdviseIndexColumn {
            /* user metadata... extract it from the system's recommended indexes */
            EXPLAIN_REQUESTER(null),
            TBCREATOR(null), 
            
            /* schema information */
            TBNAME(null), // table name (string) 
            COLNAMES(null), // '+A-B+C' means "A" ASC, "B" DESC, "C" ASC ...not sure about INCLUDE columns
            COLCOUNT(null), // #Key columns + #Include columns. Must match COLNAMES
            UNIQUERULE(null),  // 'P' (primary), 'D' (duplicates allowed), 'U' (unique) 
            UNIQUE_COLCOUNT(null), // IF unique index THEN #Key columns ELSE -1 
            REVERSE_SCANS(null), // 'Y' or 'N' indicating if reverse scans are supported
            INDEXTYPE(null), // 'CLUS', 'REG', 'DIM', 'BLOK' 
            
            /* The name of the index and the CREATE INDEX statement (must match) */
            NAME(null),  
            CREATION_TEXT(null),
            
            /* Indicates if the index is real or hypothetical */
            EXISTS(null), // 'Y' or 'N' 
            
            /* Indicates if the index is system defined... should only be true for real indexes */
            SYSTEM_REQUIRED(null), // 0, 1, or 2 
            
            /* We use this field to identify an index (also stored locally) */ 
            IID(null),
            
            /* enable the index for what-if analysis */
            /* note: not sure if this can be used to rule out real indexes??? */
            USE_INDEX(null), // 'Y' or 'N'
            
            /* statistics, set to -1 to indicate unknown */
            NLEAF("-1"), 
            NLEVELS("-1"), 
            FIRSTKEYCARD("-1"), 
            FULLKEYCARD("-1"), 
            CLUSTERRATIO("-1"), 
            AVGPARTITION_CLUSTERRATIO("-1"), 
            AVGPARTITION_CLUSTERFACTOR("-1"), 
            AVGPARTITION_PAGE_FETCH_PAIRS("''"), 
            DATAPARTITION_CLUSTERFACTOR("-1"),
            CLUSTERFACTOR("-1"), 
            SEQUENTIAL_PAGES("-1"), 
            DENSITY("-1"), 
            FIRST2KEYCARD("-1"), 
            FIRST3KEYCARD("-1"), 
            FIRST4KEYCARD("-1"), 
            PCTFREE("-1"), 
            PAGE_FETCH_PAIRS("''"), // empty string instead of -1 for this one
            MINPCTUSED("0"), // 0 instead of -1 for this one
            
            /* the rest are likely useless */
            EXPLAIN_TIME("CURRENT TIMESTAMP"), 
            CREATE_TIME("CURRENT TIMESTAMP"), 
            STATS_TIME("NULL"), 
            SOURCE_NAME("'interaction analysis tool'"), 
            REMARKS("'Created by index interaction analysis tool'"),
            CREATOR("'SYSTEM'"),        
            DEFINER("'SYSTEM'"),
            SOURCE_SCHEMA("'NULLID'"), 
            SOURCE_VERSION("''"), 
            EXPLAIN_LEVEL("'P'"), 
            USERDEFINED("1"), 
            STMTNO("1"), 
            SECTNO("1"), 
            QUERYNO("1"), 
            QUERYTAG("''"), 
            PACKED_DESC("NULL"), 
            RUN_ID("NULL"), 
            RIDTOBLOCK("'N'"), 
            CONVERTED("'Z'");
            
            // null if there is no default
            public final String defaultValue;
            
            AdviseIndexColumn(String defaultValue0) {
                defaultValue = defaultValue0;
            }
        }

        /**
         *  Contains information necessary to create a DB2 index
         *  Does NOT have all the information for creating an ADVISE_INDEX entry
         */
        public static class DB2IndexSchema
            implements Serializable, Comparable<DB2IndexSchema>
        {
            /* serializable fields */
            protected String dbName;
            protected String tableName;
            public String tableCreatorName;
            private List<Column> columns;
            // if shorter than colNames, the rest are INCLUDE columns
            // if longer than colNames, other elements are ignored
            protected List<Boolean> descending;
            protected UniqueRule uniqueRule;
            protected ReverseScanOption reverseScan;
            protected TypeOption indexType;

            /* redundant representation of the table */
            private Table table;

            /* use this to cache the signature */
            private byte[] m_signature;

            /* serialization support */
            private static final long serialVersionUID = 1L;

            /**
             * construct a new {@link DB2IndexSchema} object.
             *
             * @param dbName  database name
             * @param tableName table name
             * @param tableCreatorName  the user who created the table
             * @param colNames  column names
             * @param descending  a list of flags that signals whether the ordering of each index is descending
             * @param uniqueRule db2 unique rule
             * @param reverseScanOpt  db2 reverse scan option
             * @param indexType type of index
             * @throws SQLException
             *      unable to construct a new db2 index schema for the stated reasons.
             */
            public DB2IndexSchema(String dbName, String tableName, String tableCreatorName,
                    List<String> colNames, List<Boolean> descending, String uniqueRule,
                    String reverseScanOpt, String indexType
                    ) throws SQLException {
                this(dbName, tableName, tableCreatorName,
                        colNames, descending, UniqueRule.parse(uniqueRule),
                        ReverseScanOption.parse(reverseScanOpt),
                        TypeOption.parse(indexType)
                    );
            }

            /**
             * construct a new {@link DB2IndexSchema} object.
             *
             * @param dbName  database name
             * @param tableName table name
             * @param tableCreatorName  the user who created the table
             * @param colNames  column names
             * @param descending  a list of flags that signals whether the ordering of each index is descending
             * @param uniqueRule db2 unique rule
             * @param reverseScan  db2 reverse scan option
             * @param indexType type of index
             */
            public DB2IndexSchema(
                    String dbName,
                    String tableName,
                    String tableCreatorName,
                    List<String> colNames,
                    List<Boolean> descending,
                    UniqueRule uniqueRule,
                    ReverseScanOption reverseScan,
                    TypeOption indexType

                    ) {
                this.dbName = dbName;
                this.tableName = tableName;
                this.tableCreatorName = tableCreatorName;
                this.columns = new java.util.ArrayList<Column>(colNames.size());
                for (String name : colNames) 
                    getColumns().add(new Column(name,SQLTypes.INT));// XXX: issue #53
                this.descending = descending;
                this.uniqueRule = uniqueRule;
                this.reverseScan = reverseScan;
                this.indexType = indexType;
                this.table = new Table(this.dbName, this.tableCreatorName, this.tableName);
            }


            /**
             * returns the "Create Index ..." text for a given index name.
             * @param indexName name of index
             * @return the creation text.
             */
            public String creationText(String indexName) {
                StringBuilder sqlbuf = new StringBuilder();

                sqlbuf.append("CREATE ");
                switch (uniqueRule) {
                    case UNIQUE: sqlbuf.append("UNIQUE ");
                                 break;
                    case PRIMARY: throw new UnsupportedOperationException();
                    case NONE: break;
                    default: throw new Error("no db2 unique rule");
                }

                sqlbuf.append("INDEX ");
                Strings.formatIdentifier(indexName, sqlbuf);
                sqlbuf.append(" ON ");
                Strings.formatIdentifier(dbName, sqlbuf);
                sqlbuf.append('.');
                Strings.formatIdentifier(tableCreatorName, sqlbuf);
                sqlbuf.append('.');
                Strings.formatIdentifier(tableName, sqlbuf);

                sqlbuf.append('(');
                for (int i = 0; i < descending.size() && i < getColumns().size(); i++) {
                    if (i > 0)
                        sqlbuf.append(", ");
                    Strings.formatIdentifier(getColumns().get(i).getName(), sqlbuf);
                    sqlbuf.append(descending.get(i) ? " DESC" : " ASC");
                }
                sqlbuf.append(')');

                if (descending.size() < getColumns().size())
                {
                    /* include columns that are not sorted */
                    sqlbuf.append(" INCLUDE (");
                    for (int i = descending.size(); i < getColumns().size(); i++) {
                        if (i > descending.size())
                            sqlbuf.append(", ");
                        Strings.formatIdentifier(getColumns().get(i).getName(), sqlbuf);
                    }
                    sqlbuf.append(')');
                }

                switch (indexType) {
                    case CLUSTERING: sqlbuf.append(" CLUSTER");
                                     break;
                    case REGULAR: break;
                    case DIMENSION: throw new UnsupportedOperationException();
                    case BLOCK: throw new UnsupportedOperationException();
                    default: throw new Error("no db2 index type");
                }

                switch (reverseScan) {
                    case REVERSIBLE: sqlbuf.append(" ALLOW REVERSE SCANS");
                                     break;
                    case IRREVERSIBLE: break;
                    default: throw new Error("no db2 reverse scan option");
                }

                return sqlbuf.toString();
            }

            @Override
                public int compareTo(DB2IndexSchema other) {
                    byte[] sig1 = signature();
                    byte[] sig2 = other.signature();
                    int len = Math.min(sig1.length, sig2.length);
                    for (int i = 0; i < len; i++) {
                        if (sig1[i] != sig2[i]) {
                            return (sig1[i] < sig2[i]) ? -1 : 0;
                        }
                    }

                    if (sig1.length == sig2.length)
                        return 0;
                    else
                        return (sig1.length < sig2.length) ? -1 : 0;
                }

            @Override
                public boolean equals(Object o1) {
                    if (!(o1 instanceof DB2IndexSchema))
                        return false;

                    byte[] sig1 = signature();
                    byte[] sig2 = ((DB2IndexSchema) o1).signature();
                    if (sig1.length != sig2.length)
                        return false;
                    for (int i = 0; i < sig1.length; i++)
                        if (sig1[i] != sig2[i])
                            return false;
                    return true;
                }

            public List<Column> getColumns() {
                return columns;
            }

            public Table getBaseTable() {
                return table;
            }

            @Override
            public int hashCode() {
                return HashFunction.hashCode(signature());
            }

            private byte[] signature() {
                if (m_signature == null) {
                    StringBuilder sbuf = new StringBuilder();

                    Strings.formatIdentifier(dbName, sbuf);
                    sbuf.append('.');
                    Strings.formatIdentifier(tableCreatorName, sbuf);
                    sbuf.append('.');
                    Strings.formatIdentifier(tableName, sbuf);
                    sbuf.append('.');
                    sbuf.append(getColumns().size());
                    sbuf.append('.');
                    for (Column c : getColumns()) {
                        Strings.formatIdentifier(c.getName(), sbuf);
                        sbuf.append('.');
                    }
                    sbuf.append(descending.size());
                    sbuf.append('.');
                    for (boolean b : descending) {
                        sbuf.append(b ? '1' : '0');
                        sbuf.append('.');
                    }
                    sbuf.append(uniqueRule.toChar());
                    sbuf.append(reverseScan.toChar());
                    sbuf.append(indexType.toChar());

                    m_signature = sbuf.toString().getBytes();
                }

                return m_signature;
            }

            /**
             * UniqueRule enum.
             */
            enum UniqueRule {
                UNIQUE("U"),
                PRIMARY("P"),
                NONE("D");

                private final String code;

                UniqueRule(String code0) {
                    code = code0;
                }

                private static final Map<String,UniqueRule> codeMap;
                static {
                    codeMap = new HashMap<String,UniqueRule>();
                    for (UniqueRule type : values()) {
                        codeMap.put(type.getCode(), type);
                    }
                }

                public static UniqueRule parse(String code) throws SQLException {
                    UniqueRule type = codeMap.get(code);
                    Checks.checkSQLRelatedState(type != null, "cannot parse db2 reverse scan option: " + code);
                    if (type == null) 
                        throw new SQLException("cannot parse db2 unique rule: " + code);
                    return type;
                }

                public Object toChar() {
                    return getCode();
                }

                public String getCode() {
                    return code;
                }


                @Override
                    public String toString() {
                        return getCode();
                    }
            }

            /**
             * TypeOption enum.
             */
            enum TypeOption {
                CLUSTERING("CLUS"),
                BLOCK("BLOK"),
                DIMENSION("DIM"),
                REGULAR("REG");

                private final String code;

                TypeOption(String code) {
                    this.code = code;
                }

                private static final Map<String,TypeOption> codeMap;
                static {
                    codeMap = new HashMap<String,TypeOption>();
                    for (TypeOption type : values()) {
                        codeMap.put(type.getCode(), type);
                    }
                }

                public static TypeOption parse(String code) throws SQLException {
                    TypeOption type = codeMap.get(code);
                    Checks.checkSQLRelatedState(type != null, "cannot parse db2 reverse scan option: " + code);
                    return type;
                }

                public Object toChar() {
                    return getCode().charAt(0); // each type has a unique first character in the code
                }

                public String getCode() {
                    return code;
                }


                @Override
                public String toString() {
                    return getCode();
                }
            }

            /**
             * ReverseScanOption enum.
             */
            enum ReverseScanOption {
                REVERSIBLE("Y"),
                IRREVERSIBLE("N");

                public final String code;

                ReverseScanOption(String code) {
                    this.code = code;
                }

                private static final Map<String,ReverseScanOption> codeMap;
                static {
                    codeMap = new HashMap<String,ReverseScanOption>();
                    for (ReverseScanOption type : values()) {
                        codeMap.put(type.code, type);
                    }
                }

                public static ReverseScanOption parse(String code) throws SQLException {
                    final ReverseScanOption type = codeMap.get(code);
                    Checks.checkSQLRelatedState(type != null, "cannot parse db2 reverse scan option: " + code);
                    return type;
                }

                public Object toChar() {
                    return code;
                }
            }

        }
    }
}
