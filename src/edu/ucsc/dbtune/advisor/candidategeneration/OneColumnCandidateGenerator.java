package edu.ucsc.dbtune.advisor.candidategeneration;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.metadata.ByContentIndex;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Generate the set of candidate indexes that contain only one column; The candidates considered to 
 * be transformed into only-one-column ones are obtained from a delegate {@link CandidateGenerator}.
 *
 * @author Quoc Trung Tran
 * @author Ivo Jimenez
 */
public class OneColumnCandidateGenerator extends AbstractCandidateGenerator
{
    private CandidateGenerator delegate;

    /**
     * Constructs a generator with the given delegate used to generate candidate indexes.
     *
     * @param delegate
     *      an optimizer
     */
    public OneColumnCandidateGenerator(CandidateGenerator delegate)
    {
        this.delegate = delegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<ByContentIndex> generateByContent(SQLStatement sql) throws SQLException
    {
        Set<ByContentIndex> oneColumnIndexes = new HashSet<ByContentIndex>();

        for (Index index : delegate.generate(sql))
            for (Column col : index.columns())
                oneColumnIndexes.add(new ByContentIndex(col, index.isAscending(col)));

        return oneColumnIndexes;
    }
}
