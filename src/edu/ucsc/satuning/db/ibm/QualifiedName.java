package edu.ucsc.satuning.db.ibm;

import edu.ucsc.satuning.db.DatabaseTable;
import edu.ucsc.satuning.util.Objects;

public class QualifiedName implements DatabaseTable {
	private static final long serialVersionUID = 1L;
	
	private final String dbName;
	private final String schemaName;
	private final String name;

    /**
     * construct a qualified name instance.
     * @param dbName
     *      database name
     * @param schemaName
     *      schema name
     * @param name
     *      table name
     */
	QualifiedName(String dbName, String schemaName, String name) {
		this.dbName     = dbName;
		this.schemaName = schemaName;
		this.name       = name;
	}

    /**
     *
     * @return
     *     database name
     */
    public String getDbName() {
        return dbName;
    }

    /**
     *
     * @return
     *     database name
     */
    public String getName() {
        return name;
    }

    /**
     * 
     * @return
     *     schema name
     */
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getDbName(), getSchemaName(), getName());
    }

    public boolean equals(Object o) {
		if (!(o instanceof QualifiedName))
			return false;
		QualifiedName other = (QualifiedName) o;
		return getDbName().equals(other.getDbName())
               && getSchemaName().equals(other.getSchemaName())
               && getName().equals(other.getName());
	}

    @Override
	public String toString() {
        // todo(Huascar) can we use this ToStringBuilder here? hmm...
        return String.format("%s.%s.%s", getDbName(), getSchemaName(), getName());
	}
}
