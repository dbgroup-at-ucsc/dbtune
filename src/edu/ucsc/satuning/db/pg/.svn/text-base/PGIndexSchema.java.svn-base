package edu.ucsc.satuning.db.pg;

import edu.ucsc.satuning.db.DatabaseIndexColumn;
import edu.ucsc.satuning.db.DatabaseIndexSchema;
import edu.ucsc.satuning.db.DatabaseTable;
import edu.ucsc.satuning.util.Objects;
import edu.ucsc.satuning.util.PreConditions;
import edu.ucsc.satuning.util.ToStringBuilder;

import java.io.Serializable;
import java.util.List;

/**
 * a concreate implementation of {@link DatabaseIndexSchema} type for the PG dbms.
 */
public class PGIndexSchema implements DatabaseIndexSchema, Serializable {
	// serialization support
	private static final long serialVersionUID = 1L;

	private PGTable baseTable;
	private List<DatabaseIndexColumn> columns;
	private boolean isSync;
	private List<Boolean> isDescending;
	private String signature;

    /**
     * construct a {@code PGIndexSchema} object.
     * @param reloid
     *     relation object id? todo(Huascar) ask for the def of this...
     * @param isSync
     *    indicate whether is sync or not.
     * @param columns
     *      a list of {@link DatabaseIndexColumn columns}.
     * @param isDescending
     *      indicate whether is in descending order.
     */
    PGIndexSchema(int reloid, boolean isSync,
                  List<DatabaseIndexColumn> columns,
                  List<Boolean> isDescending
    ) {
		this.baseTable = new PGTable(reloid);
		this.isSync = isSync;
		this.columns = columns;
		this.isDescending = isDescending;

        PreConditions.checkArgument(columns.size() == isDescending.size());
		
		final StringBuilder sb = new StringBuilder();
		sb.append(reloid);
		sb.append(isSync() ? 'Y' : 'N');
		for (DatabaseIndexColumn col : columns) {
            final PGIndexColumn each = Objects.as(col);
            sb.append(each.getAttnum()).append(" ");
        }

        for (boolean d : isDescending) {
            sb.append(d ? 'y' : 'n');
        }

		this.signature = sb.toString();
	}

    /**
     * @return a {@link PGIndexSchema} duplicate.
     */
	public PGIndexSchema consDuplicate() {
        final PGTable table = Objects.as(getBaseTable());
		return new PGIndexSchema(table.getOid(), isSync(), getColumns(), getDescending());
	}

    @Override
	public boolean equals(Object that) {
		if (!(that instanceof PGIndexSchema))
			return false;
		final PGIndexSchema other = (PGIndexSchema) that;
		return getSignature().equals(other.getSignature());
	}

    @Override
    public DatabaseTable getBaseTable() {
        return baseTable;
    }

    @Override
    public List<DatabaseIndexColumn> getColumns() {
        return columns;
    }

    /**
     * @return a list of {@link Boolean} that indicate whether a {@link edu.ucsc.satuning.db.DBIndex}
     *      is listed in descending or ascending order.
     */
    List<Boolean> getDescending() {
        return isDescending;
    }

    /**
     * @return
     *      the signature of {@link edu.ucsc.satuning.db.DBIndex}
     */
    String getSignature() {
        return signature;
    }

    @Override
	public int hashCode() {
		return getSignature().hashCode();
	}

    /**
     * @return {@code true} if the index is sync; {@code false} otherwise.
     */
    boolean isSync() {
        return isSync;
    }


    @Override
    public String toString() {
        return new ToStringBuilder<PGIndexSchema>(this)
               .add("baseTable", getBaseTable())
               .add("columns", getColumns())
               .add("isSync", isSync())
               .add("isDescending", getDescending())
               .add("signature", getSignature())
               .toString();
    }
}
