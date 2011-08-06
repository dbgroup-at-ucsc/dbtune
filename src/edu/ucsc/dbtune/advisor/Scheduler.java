package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;

import java.util.List;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface Scheduler {
    Snapshot addColdCandidate(DBIndex index);
    AnalyzedQuery analyzeQuery(String sql);
    double create(DBIndex index);
    double drop(DBIndex index);
    double executeProfiledQuery(ProfiledQuery qinfo);
    List<DBIndex> getRecommendation();
    void negativeVote(DBIndex index);
    void positiveVote(DBIndex index);
    ProfiledQuery profileQuery(String sql);
    void shutdown();
    void start();
}
