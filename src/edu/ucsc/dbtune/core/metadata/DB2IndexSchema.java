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

import edu.ucsc.dbtune.core.DatabaseIndexColumn;
import edu.ucsc.dbtune.core.DatabaseIndexSchema;
import edu.ucsc.dbtune.util.DBUtilities;
import edu.ucsc.dbtune.util.HashFunction;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.util.PreConditions;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  Contains information necessary to create a DB2 index
 *  Does NOT have all the information for creating an ADVISE_INDEX entry
 */
public class DB2IndexSchema implements DatabaseIndexSchema, Serializable, Comparable<DB2IndexSchema> {
	/* serializable fields */
	protected String dbName;
	protected String tableName;
	public String tableCreatorName;
	private List<DatabaseIndexColumn> columns;
    // if shorter than colNames, the rest are INCLUDE columns
    // if longer than colNames, other elements are ignored
	protected List<Boolean> descending;
	protected UniqueRule uniqueRule;
	protected ReverseScanOption reverseScan;
	protected TypeOption indexType;
	
	/* redundant representation of the table */
    private DB2QualifiedName tableQualifiedName;
	
	/* use this to cache the signature */
	private byte[] m_signature;

	/* serialization support */
	private static final long serialVersionUID = 1L;

    /**
     * added for testing purposes.
     */
    DB2IndexSchema(String dbName, String tableName, String tableCreatorName, 
                   List<String> colNames, List<Boolean> descending, String uniqueRule,
                   String reverseScanOpt, String indexType
    ) throws SQLException {
        this(dbName, tableName, tableCreatorName,
             colNames, descending, UniqueRule.parse(uniqueRule),
             ReverseScanOption.parse(reverseScanOpt),
             TypeOption.parse(indexType)
        );
    }

    DB2IndexSchema(
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
		this.columns = new java.util.ArrayList<DatabaseIndexColumn>(colNames.size());
		for (String name : colNames) 
			getColumns().add(new DB2IndexColumn(name));
		this.descending = descending;
		this.uniqueRule = uniqueRule;
		this.reverseScan = reverseScan;
		this.indexType = indexType;
		this.tableQualifiedName = new DB2QualifiedName(this.dbName, this.tableCreatorName, this.tableName);
	}
	
	
	
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
		DBUtilities.formatIdentifier(indexName, sqlbuf);
		sqlbuf.append(" ON ");
		DBUtilities.formatIdentifier(dbName, sqlbuf);
		sqlbuf.append('.');
		DBUtilities.formatIdentifier(tableCreatorName, sqlbuf);
		sqlbuf.append('.');
		DBUtilities.formatIdentifier(tableName, sqlbuf);
		
		sqlbuf.append('(');
		for (int i = 0; i < descending.size() && i < getColumns().size(); i++) {
			if (i > 0)
				sqlbuf.append(", ");
            final DB2IndexColumn db2IndexColumn = Objects.as(getColumns().get(i));
			DBUtilities.formatIdentifier(db2IndexColumn.getName(), sqlbuf);
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
                final DB2IndexColumn includeDb2IndexColumn = Objects.as(getColumns().get(i));
				DBUtilities.formatIdentifier(includeDb2IndexColumn.getName(), sqlbuf);
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

    public List<DatabaseIndexColumn> getColumns() {
        return columns;
    }

    public DB2QualifiedName getBaseTable() {
        return tableQualifiedName;
    }
	
	public int hashCode() {
		return HashFunction.hashCode(signature());
	}
	
	private byte[] signature() {
		if (m_signature == null) {
			StringBuilder sbuf = new StringBuilder();
	
			DBUtilities.formatIdentifier(dbName, sbuf);
			sbuf.append('.');
			DBUtilities.formatIdentifier(tableCreatorName, sbuf);
			sbuf.append('.');
			DBUtilities.formatIdentifier(tableName, sbuf);
			sbuf.append('.');
			sbuf.append(getColumns().size());
			sbuf.append('.');
			for (DatabaseIndexColumn c : getColumns()) {
                final DB2IndexColumn each = Objects.as(c);
				DBUtilities.formatIdentifier(each.getName(), sbuf);
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
            PreConditions.checkSQLRelatedState(type != null, "cannot parse db2 reverse scan option: " + code);
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
            PreConditions.checkSQLRelatedState(type != null, "cannot parse db2 reverse scan option: " + code);
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
            PreConditions.checkSQLRelatedState(type != null, "cannot parse db2 reverse scan option: " + code);
	    	return type;
	    }

		public Object toChar() {
			return code;
		}
	}

}
