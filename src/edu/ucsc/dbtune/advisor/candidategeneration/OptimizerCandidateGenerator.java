package edu.ucsc.dbtune.advisor.candidategeneration;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * The class uses an Optimizer (specifically the {@link Optimizer#recommendedIndexes} method) to
 * generate candidate indexes. The set generate is, in some sense, optimal from the point of view of 
 * every single query with respect to what the optimizer thinks would be the ideal; i.e., for each 
 * statement contained in the workload, the {@link Optimizer#recommendedIndexes} method is invoked 
 * to generate candidate indexes and then union the set of candidate for each statement.
 * The repeated indexes (with the same content) are discarded.
 *
 * @author Quoc Trung Tran
 * @author Ivo Jimenez
 */
public class OptimizerCandidateGenerator extends AbstractCandidateGenerator
{
    private Optimizer optimizer;

    /**
     * Constructs a generator with the given optimizer used to generate candidate indexes.
     *
     * @param optimizer
     *      an optimizer
     */
    public OptimizerCandidateGenerator(Optimizer optimizer)
    {
        this.optimizer = optimizer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> generate(SQLStatement statement) throws SQLException
    {
        return optimizer.recommendIndexes(statement);
    }
}
