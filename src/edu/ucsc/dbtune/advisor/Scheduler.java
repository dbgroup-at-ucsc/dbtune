package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.optimizer.IBGPreparedSQLStatement;
import edu.ucsc.dbtune.metadata.Index;

import java.util.List;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface Scheduler {
    Snapshot addColdCandidate(Index index);
    AnalyzedQuery analyzeQuery(String sql);
    double create(Index index);
    double drop(Index index);
    double executeProfiledQuery(IBGPreparedSQLStatement qinfo);
    List<Index> getRecommendation();
    void negativeVote(Index index);
    void positiveVote(Index index);
    IBGPreparedSQLStatement profileQuery(String sql);
    void shutdown();
    void start();
}
