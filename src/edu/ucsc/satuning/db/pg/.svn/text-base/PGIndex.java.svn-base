package edu.ucsc.satuning.db.pg;

import edu.ucsc.satuning.db.AbstractDatabaseIndex;
import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.util.Objects;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * todo
 */
public class PGIndex extends AbstractDatabaseIndex<PGIndex> implements Serializable {
	private PGIndexSchema schema;

	// serialization support
	private static final long serialVersionUID = 1L;

    /**
     * construct a new {@code PGIndex} object.
     * @param schema
     *      a {@link PGIndexSchema} object.
     * @param internalId
     *     {@link DBIndex}'s internalId.
     * @param creationCost
     *     {@link DBIndex}'s creation cost.
     * @param megabytes
     *     {@link DBIndex}'s size (in megabytes).
     * @param creationText
     *     {@link DBIndex}'s creation text.
     */
    public PGIndex(
            PGIndexSchema schema,
            int internalId,
            double creationCost,
            double megabytes,
            String creationText
    ) {
		super(internalId, creationText, creationCost, megabytes);
        this.schema = schema;
	}

    @Override
	public PGTable baseTable() {
		return Objects.as(getSchema().getBaseTable());
	}

    @Override
	public int columnCount() {
		return getSchema().getColumns().size();
	}

    @Override
	public PGIndex consDuplicate(int id) throws SQLException {
		return new PGIndex(
                getSchema().consDuplicate(),
                id, 
                creationCost(), 
                megabytes(), 
                creationText()
        );
	}

    @Override
	public boolean equals(Object obj) {
		if (!(obj instanceof PGIndex))
			return false;
		PGIndex other = (PGIndex) obj;
		return getSchema().equals(other.getSchema());
	}

    @Override
	public PGIndexColumn getColumn(int i) {
		return Objects.as(getSchema().getColumns().get(i));
	}

    public PGIndexSchema getSchema() {
        return schema;
    }

    @Override
	public int hashCode() {
		return getSchema().hashCode();
	}

    @Override
	public String toString() {
		return creationText();
	}

}
