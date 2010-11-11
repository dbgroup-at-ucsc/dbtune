package edu.ucsc.satuning.db.pg;

import edu.ucsc.satuning.db.DatabaseIndexColumn;
import edu.ucsc.satuning.util.Objects;

import java.io.Serializable;

/**
 * todo
 */
public class PGIndexColumn implements DatabaseIndexColumn, Serializable {
    private static final long serialVersionUID = 1L;

	private final int attnum;
    PGIndexColumn(int a) {
		this.attnum = a;
	}
	
	@Override
	public boolean equals(Object other) {
        return other instanceof PGIndexColumn 
               && getAttnum() == ((PGIndexColumn) other).getAttnum();
    }

    public int getAttnum() {
        return attnum;
    }

    @Override
    public int hashCode() {
        return (31 * Objects.hashCode(attnum));
    }

    @Override
	public String toString() {
		return Integer.toString(getAttnum());
	}

}
