package edu.ucsc.dbtune.advisor.candidategeneration;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.Workload;
import edu.ucsc.dbtune.advisor.db2.DB2Advisor;

public class DB2AdvisorCandidateGenerator 
{
    protected DB2Advisor db2Advis;
    
    /**
     * Constructs a generator with the given database system used to generate candidate indexes.
     *
     * @param db2Advis
     *      An DB2 Advisor
     * @throws SQLException 
     */
    public DB2AdvisorCandidateGenerator(DB2Advisor db2Advis) throws SQLException
    {
        this.db2Advis = db2Advis;
    }
    
    
    public Set<Index> generate(Workload workload) throws SQLException 
    {
        db2Advis.process(workload);
        return db2Advis.getRecommendation(-1);
    }
}
