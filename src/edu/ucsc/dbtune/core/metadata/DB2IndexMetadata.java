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

import edu.ucsc.dbtune.core.*;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.DBUtilities;
import edu.ucsc.dbtune.util.Instances;
import edu.ucsc.dbtune.util.Objects;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DB2IndexMetadata implements Serializable {
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
	protected static final String INDEX_NAME_BASE = "recommendation_tool_index_";

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
					DBUtilities.formatStringLiteral(indexOwner, sbuf);
					break;
				case TBCREATOR:
					DBUtilities.formatStringLiteral(schema.tableCreatorName, sbuf);
					break;
				case TBNAME:
					DBUtilities.formatStringLiteral(schema.tableName, sbuf);
					break;
				case COLNAMES:
					DBUtilities.formatStringLiteral(formatColNames(schema.getColumns(), schema.descending), sbuf);
					break;
				case COLCOUNT:
					sbuf.append(schema.getColumns().size());
					break;
				case UNIQUERULE:
					DBUtilities.formatStringLiteral(schema.uniqueRule.getCode(), sbuf);
					break;
				case UNIQUE_COLCOUNT:
					sbuf.append(isUnique() ? schema.getColumns().size() : -1);
					break;
				case REVERSE_SCANS:
					DBUtilities.formatStringLiteral(schema.reverseScan.code, sbuf);
					break;
				case INDEXTYPE:
					DBUtilities.formatStringLiteral(schema.indexType.getCode(), sbuf);
					break;
				case NAME:
					DBUtilities.formatStringLiteral(indexName, sbuf);
					break;
				case CREATION_TEXT:
					DBUtilities.formatStringLiteral(creationText, sbuf);
					break;
				case EXISTS:
					DBUtilities.formatStringLiteral(indexExists, sbuf);
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
        for(DatabaseColumn each : schema.getColumns()){
            if(idx >= nSortedColumns) break;
            if(idx > 0) {
                sql.append(',');
            }

            final DB2Column db2Column = Objects.as(each);
            DBUtilities.formatIdentifier(db2Column.getName(), sql);
            idx++;
        }

		sql.append(" FROM ");
		DBUtilities.formatIdentifier(schema.dbName, sql);
		sql.append('.');
		DBUtilities.formatIdentifier(schema.tableCreatorName, sql);
		sql.append('.');
		DBUtilities.formatIdentifier(schema.tableName, sql);

		sql.append(" ORDER BY ");
		idx = 0;
		for (DatabaseColumn c : schema.getColumns()) {
			if (idx > 0) {
                sql.append(',');
            }

            final DB2Column db2Column = Objects.as(c);
            DBUtilities.formatIdentifier(db2Column.getName(), sql);
			idx++;
		}

        whatIfOptimizer.fixCandidates(Instances.<DB2Index>newLinkedList());
        return whatIfOptimizer.estimateCost(sql.toString(), Instances.newBitSet(), Instances.newBitSet());
    }
	
	public double creationCost(DatabaseConnection conn) throws SQLException {
		int i, n;
		
		StringBuilder sqlbuf = new StringBuilder();
		sqlbuf.append("SELECT ");

		i = 0; 
		n = schema.descending.size(); // n is number of *sorted* columns
		for (DatabaseColumn c : schema.getColumns()) {
			if (i >= n) break;
			if (i > 0) sqlbuf.append(',');
            final DB2Column db2Column = Objects.as(c);
			DBUtilities.formatIdentifier(db2Column.getName(), sqlbuf);
			i++;
		}
		
		sqlbuf.append(" FROM ");
		DBUtilities.formatIdentifier(schema.dbName, sqlbuf);
		sqlbuf.append('.');
		DBUtilities.formatIdentifier(schema.tableCreatorName, sqlbuf);
		sqlbuf.append('.');
		DBUtilities.formatIdentifier(schema.tableName, sqlbuf);
		
		sqlbuf.append(" ORDER BY ");
		i = 0;
		for (DatabaseColumn c : schema.getColumns()) {
			if (i > 0) sqlbuf.append(',');
            final DB2Column db2Column = Objects.as(c);
			DBUtilities.formatIdentifier(db2Column.getName(), sqlbuf);
			i++;
		}

        return calculateTotalCost(conn, sqlbuf.toString());
	}

    private static double calculateTotalCost(DatabaseConnection connection, String sql) throws SQLException {
        connection.getIBGWhatIfOptimizer().fixCandidates(Instances.<DBIndex>newLinkedList());
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

	private String formatColNames(List<DatabaseColumn> columns, List<Boolean> descending) {
		int colCount = columns.size();
		if (descending.size() != colCount)
			throw new UnsupportedOperationException("do not handle INCLUDE columns yet");
		
		StringBuilder sbuf = new StringBuilder();
		for (int i = 0; i < colCount; i++) {
			sbuf.append(descending.get(i) ? '-' : '+');
            final DB2Column each = Objects.as(columns.get(i));
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
	enum AdviseIndexColumn {
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
	    /* XXX: not sure if this can be used to rule out real indexes??? */
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
}
