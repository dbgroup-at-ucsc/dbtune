package edu.ucsc.satuning.db.pg;

import edu.ucsc.satuning.db.AbstractExplainInfo;
import edu.ucsc.satuning.util.ToStringBuilder;
import edu.ucsc.satuning.workload.SQLStatement.SQLCategory;

import java.io.Serializable;
import java.util.Arrays;

/**
 *  implements a PG-specific {@link edu.ucsc.satuning.db.ExplainInfo}.
 */
public class PGExplainInfo extends AbstractExplainInfo<PGIndex> implements Serializable {
	private static final long serialVersionUID = 1L;

	private final double[] updateCost;

    /**
     * construct a new {@code PGExplainInfo} object.
     * @param cat
     *      sql category.
     * @param overhead
     *      an array of incurred overheads.
     */
	public PGExplainInfo(SQLCategory cat, double[] overhead) {
        super(cat);
		updateCost = overhead;
	}

    @Override
	public double maintenanceCost(PGIndex index) {
		if (getSQLCategory() != SQLCategory.DML)
			return 0;
		return updateCost[index.internalId()];
	}

    @Override
    public String toString() {
        return new ToStringBuilder<PGExplainInfo>(this)
               .add("sql category", getSQLCategory())
               .add("update cost", Arrays.toString(updateCost))
               .add("isDML", isDML())
               .toString();
    }
}
