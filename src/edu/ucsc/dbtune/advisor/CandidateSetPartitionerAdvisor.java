package edu.ucsc.dbtune.advisor;

/**
 * An advisor whose candidate set is partitioned.
 *
 * @author Ivo Jimenez
 */
public interface CandidateSetPartitionerAdvisor
{
    // TODO: extract this behavior from wfit/WFIT.java
    //
    // since the partitioning behavior is also present in the RecommendationStatistics class, there 
    // has to be two interface, one for the advisor and another for the statistics.
    //
    // For now, the assumption is that all advisor might partition their candidate set. If an 
    // advisor doesn't partition it, then there's only one partition.
}
