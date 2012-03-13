package satuning.db;

import satuning.util.HashFunction;
import satuning.util.ToDoException;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

/*
 *  Contains information necessary to create a DB2 index
 *  Does NOT have all the information for creating an ADVISE_INDEX entry
 */
public class DB2IndexSchema implements Serializable, Comparable<DB2IndexSchema> {
	/* serializable fields */
	protected String dbName;
	protected String tableName;
	public String tableCreatorName;
	protected List<String> colNames;
	protected List<Boolean> descending; /* if shorter than colNames, the rest are INCLUDE columns 
	                                       if longer than colNames, other elements are ignored */
	protected UniqueRule uniqueRule;
	protected ReverseScanOption reverseScan;
	protected TypeOption indexType;
	
	/* use this to cache the signature */
	private byte[] m_signature;

	/* serialization support */
	private static final long serialVersionUID = 1L;
	protected DB2IndexSchema() { 
	}
	
	protected DB2IndexSchema(
			String dbName0,
			String tableName0,
			String tableCreatorName0,
			List<String> colNames0,
			List<Boolean> descending0, 
			UniqueRule uniqueRule0,
			ReverseScanOption reverseScan0,
			TypeOption indexType0
			) {
		dbName = dbName0;
		tableName = tableName0;
		tableCreatorName = tableCreatorName0;
		colNames = colNames0;
		descending = descending0;
		uniqueRule = uniqueRule0;
		reverseScan = reverseScan0;
		indexType = indexType0;
	}
	
	public String creationText(String indexName) {
		StringBuilder sqlbuf = new StringBuilder();
		
		sqlbuf.append("CREATE ");
		switch (uniqueRule) {
			case UNIQUE: sqlbuf.append("UNIQUE ");
			             break;
			case PRIMARY: throw new ToDoException();
			case NONE: break;
		    default: throw new Error("no db2 unique rule");
		}
		
		sqlbuf.append("INDEX ");
		DBConnection.formatIdentifier(indexName, sqlbuf);
		sqlbuf.append(" ON ");
		DBConnection.formatIdentifier(dbName, sqlbuf);
		sqlbuf.append('.');
		DBConnection.formatIdentifier(tableCreatorName, sqlbuf);
		sqlbuf.append('.');
		DBConnection.formatIdentifier(tableName, sqlbuf);
		
		sqlbuf.append('(');
		for (int i = 0; i < descending.size() && i < colNames.size(); i++) {
			if (i > 0)
				sqlbuf.append(", ");
			DBConnection.formatIdentifier(colNames.get(i), sqlbuf);
			sqlbuf.append(descending.get(i) ? " DESC" : " ASC");
		}
		sqlbuf.append(')');
		
		if (descending.size() < colNames.size())
		{
			/* include columns that are not sorted */
			sqlbuf.append(" INCLUDE (");
			for (int i = descending.size(); i < colNames.size(); i++) {
				if (i > descending.size())
					sqlbuf.append(", ");
				DBConnection.formatIdentifier(colNames.get(i), sqlbuf);
			}
			sqlbuf.append(')');
		}
		
		switch (indexType) {
			case CLUSTERING: sqlbuf.append(" CLUSTER");
			                 break;
			case REGULAR: break;
			case DIMENSION: throw new ToDoException();
			case BLOCK: throw new ToDoException();
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
	
	private byte[] signature() {
		if (m_signature == null) {
			StringBuilder sbuf = new StringBuilder();
	
			DBConnection.formatIdentifier(dbName, sbuf);
			sbuf.append('.');
			DBConnection.formatIdentifier(tableCreatorName, sbuf);
			sbuf.append('.');
			DBConnection.formatIdentifier(tableName, sbuf);
			sbuf.append('.');
			sbuf.append(colNames.size());
			sbuf.append('.');
			for (String s : colNames) {
				DBConnection.formatIdentifier(s, sbuf);
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
	
	public int hashCode() {
		return HashFunction.hashCode(signature());
	}
	
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
	
	enum UniqueRule {
		UNIQUE("U"),
		PRIMARY("P"),
		NONE("D");
		
		public final String code;
		
		UniqueRule(String code0) {
			code = code0;
		}
		
	    private static final java.util.HashMap<String,UniqueRule> codeMap;
	    static {
	    	codeMap = new java.util.HashMap<String,UniqueRule>();
	    	for (UniqueRule type : values()) {
	    		codeMap.put(type.code, type);
	    	}
	    }
	    
	    public static final UniqueRule parse(String code) throws SQLException {
	    	UniqueRule type = codeMap.get(code);
	    	if (type == null) 
	    		throw new SQLException("cannot parse db2 unique rule: " + code);
	    	return type;
	    }

		public Object toChar() {
			return code;
		}
	}
	
	enum TypeOption {
	    CLUSTERING("CLUS"),
	    BLOCK("BLOK"),
	    DIMENSION("DIM"),
	    REGULAR("REG");
	    
	    public final String code;
	    
	    TypeOption(String code0) {
	    	code = code0;
	    }
	    
	    private static final java.util.HashMap<String,TypeOption> codeMap;
	    static {
	    	codeMap = new java.util.HashMap<String,TypeOption>();
	    	for (TypeOption type : values()) {
	    		codeMap.put(type.code, type);
	    	}
	    }
	    
	    public static final TypeOption parse(String code) throws SQLException {
	    	TypeOption type = codeMap.get(code);
	    	if (type == null) 
	    		throw new SQLException("cannot parse db2 index type: " + code);
	    	return type;
	    }

		public Object toChar() {
			return code.charAt(0); // each type has a unique first character in the code
		}
	}
	
	enum ReverseScanOption {
	    REVERSIBLE("Y"),
	    IRREVERSIBLE("N");
	    
	    public final String code;
	    
	    ReverseScanOption(String code0) {
	    	code = code0;
	    }
	    
	    private static final java.util.HashMap<String,ReverseScanOption> codeMap;
	    static {
	    	codeMap = new java.util.HashMap<String,ReverseScanOption>();
	    	for (ReverseScanOption type : values()) {
	    		codeMap.put(type.code, type);
	    	}
	    }
	    
	    public static final ReverseScanOption parse(String code) throws SQLException {
	    	ReverseScanOption type = codeMap.get(code);
	    	if (type == null) 
	    		throw new SQLException("cannot parse db2 reverse scan option: " + code);
	    	return type;
	    }

		public Object toChar() {
			return code;
		}
	}

}
