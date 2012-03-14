package satuning.db;

import satuning.db.DB2IndexSchema.UniqueRule;
import satuning.db.DB2IndexSchema.ReverseScanOption;
import satuning.db.DB2IndexSchema.TypeOption;
import satuning.util.BitSet;
import satuning.util.ToDoException;

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
	protected static final String indexNameBase = "recommendation_tool_index_";

	// serialization support
	private static final long serialVersionUID = 1L;
	protected DB2IndexMetadata() {
	}
	
	private DB2IndexMetadata(
			DB2IndexSchema schema0,
			int internalId0,
			String indexName0,
			String indexOwner0,
			String indexExists0,
			int systemRequired0,
			double megabytes0) 
	throws SQLException {
		schema = schema0;
		internalId = internalId0;
		creationText = schema.creationText(indexName0);
		indexName = indexName0;
		indexOwner = indexOwner0;
		indexExists = indexExists0;
		systemRequired = systemRequired0;
		megabytes = megabytes0; 
		
		if (indexExists0.equals("Y"))
			throw new ToDoException("do not handle existing indexes yet");
	}
	
	public static DB2IndexMetadata consDuplicate(DB2IndexMetadata original, int id) throws SQLException {		
		// construct the object
		String indexName = indexNameBase + id;
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
		
		UniqueRule uniqueRule = UniqueRule.NONE;
		ReverseScanOption reverseScan = ReverseScanOption.REVERSIBLE;
		TypeOption indexType = TypeOption.REGULAR;
		
		DB2IndexSchema schema = new DB2IndexSchema(dbName, tableName, tableCreator, colNames, descending, 
												   uniqueRule, reverseScan, indexType);
		
		// construct the object
		String indexName = indexNameBase + id;
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
		UniqueRule uniqueRule = UniqueRule.parse(uniqueRuleString);

		String reverseScanString = rs.getString(AdviseIndexColumn.REVERSE_SCANS.ordinal() + 1);
		ReverseScanOption reverseScan = ReverseScanOption.parse(reverseScanString);

		String indexTypeString = rs.getString(AdviseIndexColumn.INDEXTYPE.ordinal() + 1);
		TypeOption indexType = TypeOption.parse(indexTypeString);
		
		DB2IndexSchema schema = new DB2IndexSchema(dbName, tableName, tableCreator, colNames, descending, 
												   uniqueRule, reverseScan, indexType);
		
		// construct the object
		String indexName = indexNameBase + id;
		String indexOwner = rs.getString(AdviseIndexColumn.EXPLAIN_REQUESTER.ordinal() + 1);
		String indexExists = rs.getString(AdviseIndexColumn.EXISTS.ordinal() + 1);
		int systemRequired = rs.getInt(AdviseIndexColumn.SYSTEM_REQUIRED.ordinal() + 1);
		
		return new DB2IndexMetadata(schema, id, indexName, indexOwner, indexExists, systemRequired, megabytes);
	}
	
	public String toString() {
		return creationText;
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
	
	public double creationCost(DBConnection conn) throws SQLException {
		int i, n;
		
		StringBuilder sqlbuf = new StringBuilder();
		sqlbuf.append("SELECT ");

		i = 0; 
		n = schema.descending.size(); // n is number of *sorted* columns
		for (String col : schema.colNames) {
			if (i >= n) break;
			if (i > 0) sqlbuf.append(',');
			DBConnection.formatIdentifier(col, sqlbuf);
			i++;
		}
		
		sqlbuf.append(" FROM ");
		DBConnection.formatIdentifier(schema.dbName, sqlbuf);
		sqlbuf.append('.');
		DBConnection.formatIdentifier(schema.tableCreatorName, sqlbuf);
		sqlbuf.append('.');
		DBConnection.formatIdentifier(schema.tableName, sqlbuf);
		
		sqlbuf.append(" ORDER BY ");
		i = 0;
		for (String col : schema.colNames) {
			if (i > 0) sqlbuf.append(',');
			DBConnection.formatIdentifier(col, sqlbuf);
			i++;
		}

		conn.fixCandidates(new java.util.LinkedList<DB2Index>());
		double queryCost = conn.whatifOptimize(sqlbuf.toString(), new BitSet(), new BitSet());
		return queryCost;
	}

	@Override
	public boolean equals(Object o1) {
		if (!(o1 instanceof DB2IndexMetadata))
			return false;
		return ((DB2IndexMetadata) o1).schema.equals(schema);
	}
	
	@Override
	public int hashCode() {
		return schema.hashCode();
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
					DBConnection.formatStringLiteral(indexOwner, sbuf);
					break;
				case TBCREATOR:
					DBConnection.formatStringLiteral(schema.tableCreatorName, sbuf);
					break;
				case TBNAME:
					DBConnection.formatStringLiteral(schema.tableName, sbuf);
					break;
				case COLNAMES:
					DBConnection.formatStringLiteral(formatColNames(schema.colNames, schema.descending), sbuf);
					break;
				case COLCOUNT:
					sbuf.append(schema.colNames.size());
					break;
				case UNIQUERULE:
					DBConnection.formatStringLiteral(schema.uniqueRule.code, sbuf);
					break;
				case UNIQUE_COLCOUNT:
					sbuf.append(isUnique() ? schema.colNames.size() : -1);
					break;
				case REVERSE_SCANS:
					DBConnection.formatStringLiteral(schema.reverseScan.code, sbuf);
					break;
				case INDEXTYPE:
					DBConnection.formatStringLiteral(schema.indexType.code, sbuf);
					break;
				case NAME:
					DBConnection.formatStringLiteral(indexName, sbuf);
					break;
				case CREATION_TEXT:
					DBConnection.formatStringLiteral(creationText, sbuf);
					break;
				case EXISTS:
					DBConnection.formatStringLiteral(indexExists, sbuf);
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

	private String formatColNames(List<String> colNames, List<Boolean> descending) {
		int colCount = colNames.size();
		if (descending.size() != colCount)
			throw new ToDoException("do not handle INCLUDE columns yet");
		
		StringBuilder sbuf = new StringBuilder();
		for (int i = 0; i < colCount; i++) {
			sbuf.append(descending.get(i) ? '-' : '+');
			sbuf.append(colNames.get(i));
		}
		
		return sbuf.toString();
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
}
