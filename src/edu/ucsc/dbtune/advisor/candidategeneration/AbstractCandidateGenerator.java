package edu.ucsc.dbtune.advisor.candidategeneration;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

import edu.ucsc.dbtune.metadata.ByContentIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;
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
     * {@inheritDoc}
     */
    @Override
    public Set<Index> generate(SQLStatement statement) throws SQLException
    {
        return Sets.<Index>newHashSet(generateByContent(statement));
    }

    /**
     * Generates candidates for the given workload. The generated set contains unique indexes, where 
     * the uniqueness is determined by the {@link ByContentIndex} class, i.e. two indexes with the 
     * same properties are considered equal, even if their names differ (as it is the case with the 
     * "regular" {@link Index} class, where the fully qualified name of the index is used to 
     * uniquely identify an index).
     *
     * @param workload
     *      the workload for which to generate candidates
     * @return
     *      a candidate set for the given workload
     * @throws SQLException
     *      if the generation of candidates can't be done
     */
    public Set<ByContentIndex> generateByContent(Workload workload) throws SQLException
    {
        Set<ByContentIndex> generated = new HashSet<ByContentIndex>();

        for (SQLStatement sql : workload)
            generated.addAll(generateByContent(sql));

        return generated;
    }

    /**
     * Generates candidates for the given statement. The generated set contains unique indexes, 
     * where the uniqueness is determined by the {@link ByContentIndex} class, i.e. two indexes with 
     * the same properties are considered equal, even if their names differ (as it is the case with 
     * the "regular" {@link Index} class, where the fully qualified name of the index is used to 
     * uniquely identify an index).
     *
     * @param statement
     *      the statement for which to generate candidates
     * @return
     *      a candidate set for the given workload
     * @throws SQLException
     *      if the generation of candidates can't be done
     */
    public abstract Set<ByContentIndex> generateByContent(SQLStatement statement)
        throws SQLException;
}
