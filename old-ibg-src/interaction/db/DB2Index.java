package interaction.db;

import interaction.util.ToDoException;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/*
 * XXX: this may be inferred by playing with db2advis and watching the ADVISE_INDEX contents
 */
public class DB2Index implements Serializable {
	// serialized fields
	protected DB2IndexSchema schema;
	protected int internalID;
	protected String indexName;
	protected String indexOwner;
	protected String tableOwner;
	protected String indexExists;
	protected int systemRequired;
	protected String creationText;
	
	// for index names
	protected static final String indexNameBase = "interaction_tool_index_";

	// serialization support
	private static final long serialVersionUID = 3894859060945294518L;
	protected DB2Index() {
	}
	
	public DB2Index(
			DB2IndexSchema schema0,
			int internalID0,
			String indexName0,
			String indexOwner0,
			String tableOwner0,
			String indexExists0,
			int systemRequired0) 
	throws SQLException {
		schema = schema0;
		creationText = schema.creationText(indexName0);
		internalID = internalID0;
		indexName = indexName0;
		indexOwner = indexOwner0;
		tableOwner = tableOwner0;
		indexExists = indexExists0;
		systemRequired = systemRequired0; 
		
		if (indexExists0.equals("Y"))
			throw new ToDoException("do not handle existing indexes yet");
	}

	/*
	 * Returns true if the index has unique keys.
	 * Both unique and primary indexes have unique keys.
	 */
	public boolean isUnique() {
		char u = schema.uniqueRule.charAt(0);
		return u == 'P' || u == 'U';
	}
	
	public String creationText() {
		return creationText;
	}
	
	public static DB2IndexSet extractSingleColumns(DB2IndexSet originals) throws SQLException {
		DB2IndexSet result = new DB2IndexSet();
		
		int id = 0;
		for (DB2Index multiIndex : originals) {
			for (String col : multiIndex.schema.colNames) {
				result.add(consSingleColumn(multiIndex, col, id));
				++id;
			}
		}
		
		result.normalize();
		return result;
	}
	
	public static DB2Index consSingleColumn(DB2Index original, String col, int id) throws SQLException {
		// construct the schema
		String dbName = original.schema.dbName;
		String tableName = original.schema.tableName;
		
		List<String> colNames = new ArrayList<String>();
		colNames.add(col);
		
		List<Boolean> descending = new ArrayList<Boolean>();
		descending.add(false);
		
		String uniqueRule = "D";
		String reverseScan = "Y";
		String indexType = "REG";
		
		DB2IndexSchema schema = new DB2IndexSchema(dbName, tableName, colNames, descending, 
												   uniqueRule, reverseScan, indexType);
		
		// construct the object
		String indexName = indexNameBase + id;
		String indexOwner = original.indexOwner;
		String tableOwner = original.tableOwner;
		String indexExists = "N";
		int systemRequired = 0;
		
		return new DB2Index(schema, id, indexName, indexOwner, tableOwner, indexExists, systemRequired);
	}
	
	/* construct an index from the ADVISE_INDEX table */
	public static DB2Index consFromAdviseIndex(ResultSet rs, String dbName, int id) throws SQLException {
		// construct the schema
		String tableName = rs.getString(AdviseIndexColumn.TBNAME.ordinal() + 1);
		
		String colNamesString = rs.getString(AdviseIndexColumn.COLNAMES.ordinal() + 1);
		List<String> colNames = new ArrayList<String>(); 
		List<Boolean> descending = new ArrayList<Boolean>();
		parseColNames(colNamesString, colNames, descending);
		
		String uniqueRule = rs.getString(AdviseIndexColumn.UNIQUERULE.ordinal() + 1);
		String reverseScan = rs.getString(AdviseIndexColumn.REVERSE_SCANS.ordinal() + 1);
		String indexType = rs.getString(AdviseIndexColumn.INDEXTYPE.ordinal() + 1);
		
		DB2IndexSchema schema = new DB2IndexSchema(dbName, tableName, colNames, descending, 
												   uniqueRule, reverseScan, indexType);
		
		// construct the object
		String indexName = indexNameBase + id;
		String indexOwner = rs.getString(AdviseIndexColumn.EXPLAIN_REQUESTER.ordinal() + 1);
		String tableOwner = rs.getString(AdviseIndexColumn.TBCREATOR.ordinal() + 1);
		String indexExists = rs.getString(AdviseIndexColumn.EXISTS.ordinal() + 1);
		int systemRequired = rs.getInt(AdviseIndexColumn.SYSTEM_REQUIRED.ordinal() + 1);
		
		return new DB2Index(schema, id, indexName, indexOwner, tableOwner, indexExists, systemRequired);
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
	
	public String toString() {
		return creationText;
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
					DBConnection.formatStringLiteral(tableOwner, sbuf);
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
					DBConnection.formatStringLiteral(schema.uniqueRule, sbuf);
					break;
				case UNIQUE_COLCOUNT:
					sbuf.append(isUnique() ? schema.colNames.size() : -1);
					break;
				case REVERSE_SCANS:
					DBConnection.formatStringLiteral(schema.reverseScan, sbuf);
					break;
				case INDEXTYPE:
					DBConnection.formatStringLiteral(schema.indexType, sbuf);
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
					sbuf.append(internalID);
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

	public boolean equals(Object o1) {
		if (!(o1 instanceof DB2Index))
			return false;
		return ((DB2Index) o1).schema.equals(schema);
	}
	
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
	
	/**
	 * Construct an index from the DBTune table 
	 * 
	 */
    public static DB2Index constructIndexFromDBTune 
                    (String dbName, String tableName, 
                    List<String> colNames, List<Boolean> descending,
                    int id)  throws SQLException 
    {
        
        // construct the schema
        String uniqueRule = "D";
        String reverseScan = "Y";
        String indexType = "REG";
        
        DB2IndexSchema schema = new DB2IndexSchema(dbName, tableName, colNames, descending, 
                                                   uniqueRule, reverseScan, indexType);
        
        // construct the object
        String indexName = indexNameBase + id;
        String indexOwner = "DB2INST1";
        String tableOwner = tableName;
        String indexExists = "N";
        int systemRequired = 0;
        
        return new DB2Index(schema, id, indexName, indexOwner, tableOwner, indexExists, systemRequired);
    }
}
