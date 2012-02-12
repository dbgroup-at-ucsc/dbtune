package edu.ucsc.dbtune.candidategeneration;

import java.sql.SQLException;
import java.util.Set;

import com.google.common.collect.Sets;

import edu.ucsc.dbtune.metadata.ByContentIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.Workload;

/**
 * A base implementation of the candidate generation interface that requires its implementations to 
 * only implement the {@link #generateByContent} method.
 *
 * @author Ivo Jimenez
 */
public abstract class AbstractCandidateGenerator implements CandidateGenerator
{
    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> generate(Workload workload) throws SQLException
    {
        return Sets.<Index>newHashSet(generateByContent(workload));
    }

    /**
     * Generates candidates for the given workload. The generated set contains unique indexes, where 
     * the uniqueness is identified by using the {@link ByContentIndex} class, i.e. indexes 
     * contained in the set are all different among them with respect to the columns contained on 
     * each and their respective ordering (as opposed to be different with respect to the fully 
     * qualified name of the index, i.e. as in the {@link Index} class).
     *
     * @param workload
     *      the workload for which to generate candidates
     * @return
     *      a candidate set for the given workload
     * @throws SQLException
     *      if the generation of candidates can't be done
     */
    public abstract Set<ByContentIndex> generateByContent(Workload workload) throws SQLException;
}
