package edu.ucsc.dbtune.advisor.wfit;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.advisor.interactions.InteractionBank;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;

import static edu.ucsc.dbtune.advisor.wfit.SATuningDBTuneTranslator.toSet;

//CHECKSTYLE:OFF
public class ProfiledQuery implements Serializable {
    private static final long serialVersionUID = 1L;
    ProfiledQuery() { }
    
    public String sql;
    public Set<Index> candidateSet;
    public ExplainedSQLStatement explainInfo;
    public PreparedSQLStatement pStmt;
    public InteractionBank bank;
    public int whatifCount; // value from DBConnection after profiling
    public int idOffset;
    
    public ProfiledQuery(
            String sql0,
            PreparedSQLStatement pStmt0,
            ExplainedSQLStatement explainInfo0,
            Set<Index> candidateSet0,
            InteractionBank bank0,
            int idOffset0)
    {
        sql = sql0;
        explainInfo = explainInfo0;
        pStmt = pStmt0;
        candidateSet = candidateSet0;
        bank = bank0;
        idOffset = idOffset0;
    }
    
    public double cost(BitSet config) {
        return cost(toSet(config, candidateSet, idOffset));
    }
    
    public double cost(Set<Index> config) {
        double total;
        try {
            total = pStmt.explain(config).getTotalCost();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return total;
    }
}
//CHECKSTYLE:ON
