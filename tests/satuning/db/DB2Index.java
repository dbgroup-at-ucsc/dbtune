package satuning.db;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Comparator;

public class DB2Index implements Serializable {
	// serialized fields
	protected DB2IndexMetadata meta;
	protected double creationCost;
	private int hashCodeCache;

	// serialization support
	private static final long serialVersionUID = 1L;
	protected DB2Index() {
	}
	
	public DB2Index(DB2IndexMetadata meta0,	double creationCost0) throws SQLException {
		meta = meta0;
		creationCost = creationCost0;
		hashCodeCache = meta0.hashCode();
	}

	public double creationCost() {
		return creationCost;
	}
	
	public String creationText() {
		return meta.creationText;
	}

	public String tableName() {
		return meta.schema.tableName;
	}
	
	public String tableSchemaName() {
		return meta.schema.tableCreatorName;
	}

	public int internalId() {
		return meta.internalId;
	}
	
	public String toString() {
		return meta.creationText;
	}
	
	public int columnCount() {
		return meta.schema.colNames.size();
	}
	
	public String columnName(int i) {
		return meta.schema.colNames.get(i);
	}
	
	public boolean equals(Object o1) {
		if (!(o1 instanceof DB2Index))
			return false;
		return ((DB2Index) o1).meta.equals(meta);
	}
	
	public int hashCode() {
		return hashCodeCache;
	}
	
	/*
	 * java.util.Comparator for displaying indexes in an easy to read format
	 */
	public static Comparator<DB2Index> schemaComparator = new Comparator<DB2Index>() {
		public int compare(DB2Index o1, DB2Index o2) {
			return o1.meta.schema.compareTo(o2.meta.schema);
		}
	};
	public static DB2Index consDuplicate(DB2Index index, int id) throws SQLException {
		DB2IndexMetadata meta = DB2IndexMetadata.consDuplicate(index.meta, id);
		DB2Index dup = new DB2Index(meta, index.creationCost);
		return dup;
	}

	public boolean isOn(QualifiedName name) {
		return name.equals(meta.schema.dbName, meta.schema.tableCreatorName, meta.schema.tableName);
	}

	public double megabytes() {
		if (meta.megabytes < 0)
			throw new AssertionError("Index size is not known");
		return meta.megabytes;
	}
}
