package satuning.db;

import java.io.Serializable;

import satuning.util.Debug;
import satuning.workload.SQLStatement.SQLCategory;

public class ExplainInfo implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final SQLCategory category;
	private final QualifiedName updatedTable;
	private final double updateCost;
	
	public ExplainInfo(SQLCategory cat, QualifiedName updTable, double updCost) {
		if (cat == SQLCategory.DML) {
			Debug.assertion(updTable != null, "need updated table for DML");
			Debug.assertion(updCost >= 0, "invalid update cost");
		}
		category = cat;
		updatedTable = updTable;
		updateCost = updCost;
	}

	public double maintenanceCost(DB2Index index) {
		if (category != SQLCategory.DML)
			return 0;
		if (!index.isOn(updatedTable))
			return 0;
		return updateCost;
	}

	public boolean isDML() {
		return category == SQLCategory.DML;
	}

	public void trimUpdatedTable() {
		if (updatedTable != null) {
			Debug.print("\"" + updatedTable + "\" => ");
			updatedTable.name = updatedTable.name.trim();
			updatedTable.schemaName = updatedTable.schemaName.trim();
			updatedTable.dbName = updatedTable.dbName.trim();
			Debug.println("\"" + updatedTable + "\"");
		}
	}
}
