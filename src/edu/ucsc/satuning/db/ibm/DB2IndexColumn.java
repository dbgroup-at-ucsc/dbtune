package edu.ucsc.satuning.db.ibm;

import java.io.Serializable;

import edu.ucsc.satuning.db.DatabaseIndexColumn;
import edu.ucsc.satuning.util.Objects;

public class DB2IndexColumn implements DatabaseIndexColumn, Serializable {
	private static final long serialVersionUID = 1L;
	private String name;
	
	DB2IndexColumn(String n) {
		this.name = n;
	}
	
	@Override
	public boolean equals(Object other) {
        return other instanceof DB2IndexColumn 
               && getName().equals(((DB2IndexColumn) other).getName());
    }

    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        return 34 * Objects.hashCode(getName());
    }

    @Override
	public String toString() {
		return getName();
	}

}
