package edu.ucsc.satuning.db.ibm;

import edu.ucsc.satuning.db.AbstractExplainInfo;
import edu.ucsc.satuning.util.Debug;
import edu.ucsc.satuning.util.ToStringBuilder;
import edu.ucsc.satuning.workload.SQLStatement.SQLCategory;

import java.io.Serializable;

/**
 *  implements a DB2-specific {@link edu.ucsc.satuning.db.ExplainInfo}.
 */
public class DB2ExplainInfo extends AbstractExplainInfo<DB2Index> implements Serializable{
	private static final long serialVersionUID = 1L;
	private final QualifiedName updatedTable;
	private final double updateCost;

    /**
     * construct a {@code DB2ExplainInfo} object.
     * @param cat
     *      sql category
     * @param updTable
     *      updated table
     * @param updCost
     *      update cost.
     */
	public DB2ExplainInfo(SQLCategory cat, QualifiedName updTable, double updCost) {
        super(cat);
		if (cat == SQLCategory.DML) {
			Debug.assertion(updTable != null, "need updated table for DML");
			Debug.assertion(updCost >= 0, "invalid update cost");
		}
		updatedTable = updTable;
		updateCost = updCost;
	}

    @Override
	public double maintenanceCost(DB2Index index) {
		if (getSQLCategory() != SQLCategory.DML)
			return 0;
		if (!index.isOn(updatedTable))
			return 0;
		return updateCost;
	}

    @Override
    public String toString() {
        return new ToStringBuilder<DB2ExplainInfo>(this)
               .add("update table", updatedTable)
               .add("sql category", getSQLCategory())
               .add("update cost", updateCost)
               .add("isDML", isDML())
               .toString();
    }
}
