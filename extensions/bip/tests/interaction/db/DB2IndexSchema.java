package interaction.db;

import interaction.util.HashFunction;
import interaction.util.ToDoException;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/*
 *  Contains information necessary to create a DB2 index
 *  Does NOT have all the information for creating an ADVISE_INDEX entry
 */
public class DB2IndexSchema implements Serializable {
	/* serializable fields */
	protected String dbName;
	protected String tableName;
	protected List<String> colNames;
	protected List<Boolean> descending; /* if shorter than colNames, the rest are INCLUDE columns */
	protected String uniqueRule;
	protected String reverseScan;
	protected String indexType;
	
	/* use this to cache the signature */
	private byte[] m_signature;

	/* serialization support */
	private static final long serialVersionUID = 3894859060945294518L;
	protected DB2IndexSchema() { 
	}
	
	public String getTableName()
	{
	    return tableName;
	}
	
	public List<String> getColumnNames()
	{
	    return colNames;
	}
	
	public List<Boolean> getAscending()
	{
	    List<Boolean> ascending = new ArrayList<Boolean>();
	    
	    for (Boolean b : descending)
	        ascending.add(!b);
	    
	    return ascending;
	}
	
	protected DB2IndexSchema(
			String dbName0,
			String tableName0,
			List<String> colNames0,
			List<Boolean> descending0, 
			String uniqueRule0,
			String reverseScan0,
			String indexType0
			) {
		dbName = dbName0;
		tableName = tableName0;
		colNames = colNames0;
		descending = descending0;
		uniqueRule = uniqueRule0;
		reverseScan = reverseScan0;
		indexType = indexType0;
	}
	
	public String creationText(String indexName) throws SQLException {
		StringBuilder sqlbuf = new StringBuilder();
		
		sqlbuf.append("CREATE ");
		if (uniqueRule.equals("U"))
			sqlbuf.append("UNIQUE ");
		else if (uniqueRule.equals("P"))
			throw new ToDoException();
		else if (!uniqueRule.equals("D"))
			throw new SQLException("wrong format of unique rule: " + uniqueRule);
		sqlbuf.append("INDEX ");
		DBConnection.formatIdentifier(indexName, sqlbuf);
		sqlbuf.append(" ON ");
		DBConnection.formatIdentifier(dbName, sqlbuf);
		sqlbuf.append('.');
		DBConnection.formatIdentifier(tableName, sqlbuf);
		
		if (descending.size() > colNames.size()) // protect against exceptions in loop
			throw new SQLException("descending list is too long");
		
		sqlbuf.append('(');
		for (int i = 0; i < descending.size(); i++) {
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
		
		if (indexType.equals("CLUS")) 
			sqlbuf.append(" CLUSTER");
		if (indexType.equals("DIM") ||indexType.equals("BLOK")) 
			throw new ToDoException();
		else if (!indexType.equals("REG"))
			throw new SQLException("unexpected index type: " + indexType);
		
		if (reverseScan.equals("Y"))
			sqlbuf.append(" ALLOW REVERSE SCANS");
		
		return sqlbuf.toString();
	}
	
	private byte[] signature() {
		if (m_signature == null) {
			StringBuilder sbuf = new StringBuilder();
	
			DBConnection.formatIdentifier(dbName, sbuf);
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
			sbuf.append(uniqueRule); // single char
			sbuf.append(reverseScan); // single char
			sbuf.append(indexType);
			
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
}
