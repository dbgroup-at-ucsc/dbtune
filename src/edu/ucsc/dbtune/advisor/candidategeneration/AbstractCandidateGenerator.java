package edu.ucsc.dbtune.advisor.candidategeneration;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

/**
 * A base implementation of the candidate generation interface that requires its implementations to 
 * only implement the {@link #generate(SQLStatement)} method.
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
        Set<Index> generated = new HashSet<Index>();

        for (SQLStatement sql : workload)
            generated.addAll(generate(sql));

        return generated;
    }
}
