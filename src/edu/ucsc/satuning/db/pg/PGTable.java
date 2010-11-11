package edu.ucsc.satuning.db.pg;

import edu.ucsc.satuning.db.DatabaseTable;
import edu.ucsc.satuning.util.Objects;
import edu.ucsc.satuning.util.ToStringBuilder;

public class PGTable implements DatabaseTable {
    private static final long serialVersionUID = 1L;
	private final int oid;

    /**
     *
     * @param o
     */
    PGTable(int o) {
		oid = o;
	}

    @Override
	public boolean equals(Object other) {
        return other instanceof PGTable
               && ((PGTable) other).getOid() == getOid();
    }

    /**
     *
     * @return
     */
    public int getOid() {
        return oid;
    }

    @Override
    public int hashCode() {
        return 34 * Objects.hashCode(getOid());
    }

	@Override
	public String toString() {
		return new ToStringBuilder<PGTable>(this)
               .add("oid", getOid())
               .toString();
	}

}
