package edu.ucsc.dbtune.advisor.wfit;

import java.io.Serializable;

import java.sql.SQLException;

import java.util.Set;

import edu.ucsc.dbtune.advisor.interactions.InteractionBank;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;

import static edu.ucsc.dbtune.util.MetadataUtils.toSet;

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
    
    public ProfiledQuery(
            String sql0,
            PreparedSQLStatement pStmt0,
            ExplainedSQLStatement explainInfo0,
            Set<Index> candidateSet0,
            InteractionBank bank0,
            int whatifCount0)
    {
        sql = sql0;
        explainInfo = explainInfo0;
        pStmt = pStmt0;
        candidateSet = candidateSet0;
        bank = bank0;
        whatifCount = whatifCount0;
    }
    
    public double cost(BitSet config) {
        double total;
        try {
            total = pStmt.explain(toSet(config, candidateSet)).getTotalCost();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return total;
    }
}
//CHECKSTYLE:ON
