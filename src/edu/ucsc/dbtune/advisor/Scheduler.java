package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;

import java.util.List;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface Scheduler <I extends DBIndex> {
    Snapshot<I> addColdCandidate(I index);
    AnalyzedQuery<I> analyzeQuery(String sql);
    double create(I index);
    double drop(I index);
    double executeProfiledQuery(ProfiledQuery<I> qinfo);
    List<I> getRecommendation();
    void negativeVote(I index);
    void positiveVote(I index);
    ProfiledQuery<I> profileQuery(String sql);
    void shutdown();
    void start();
}
