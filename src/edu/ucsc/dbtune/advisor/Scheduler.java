package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.metadata.Index;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;

import java.util.List;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface Scheduler {
    Snapshot addColdCandidate(Index index);
    AnalyzedQuery analyzeQuery(String sql);
    double create(Index index);
    double drop(Index index);
    double executeProfiledQuery(ProfiledQuery qinfo);
    List<Index> getRecommendation();
    void negativeVote(Index index);
    void positiveVote(Index index);
    ProfiledQuery profileQuery(String sql);
    void shutdown();
    void start();
}
