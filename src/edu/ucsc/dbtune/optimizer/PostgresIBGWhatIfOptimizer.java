package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.sql.SQLException;
import java.util.List;

import static edu.ucsc.dbtune.connectivity.PGCommands.explainIndexesCost;
import static edu.ucsc.dbtune.spi.core.Functions.supplyValue;
import static edu.ucsc.dbtune.util.Instances.newBitSet;
import static edu.ucsc.dbtune.util.Instances.newList;

/**
 * A Postgres-specific What-if optimizer.
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class PostgresIBGWhatIfOptimizer extends AbstractIBGWhatIfOptimizer {
    private final DatabaseConnection connection;
    private final IndexBitSet        cachedBitSet       = newBitSet();
    private final List<Index>      cachedCandidateSet = newList();

    /**
     * construct a {@code Postgres-WhatIfOptimizer} object.
     * @param connection
     *      a database connection
     */
    public PostgresIBGWhatIfOptimizer(DatabaseConnection connection){
        super(new PGOptimizer(connection));
        this.connection = connection;
    }

    public void fixCandidates(Iterable<? extends Index> candidateSet) throws SQLException {
        cachedCandidateSet.clear();
        getCachedIndexBitSet().clear();
        for (Index idx : candidateSet) {
            cachedCandidateSet.add(idx);
            getCachedIndexBitSet().set(idx.getId());
        }
    }

    protected IndexBitSet getCachedIndexBitSet() {
        return cachedBitSet;
    }

    protected void updateCachedIndexBitSet(IndexBitSet newOne) {
        cachedBitSet.set(newOne);
    }

    @Override
    public ExplainInfo explain(String sql, Iterable<? extends Index> indexes) throws SQLException {
        fixCandidates(indexes);
        return delegate.explain(sql, indexes);
    }

    @Override
    public Iterable<Index> getCandidateSet() {
        return cachedCandidateSet;
    }

    @Override
    double estimateCost(String sql, Iterable<Index> candidate,
        IndexBitSet configuration, IndexBitSet used) {
      return supplyValue(
          explainIndexesCost(used),
          connection,
          cachedCandidateSet,
          configuration,
          sql,
          configuration.cardinality(),
          new Double[cachedCandidateSet.size()]
      );
    }

    @Override
    protected double estimateCost(WhatIfOptimizationBuilder builder) throws SQLException {
        whatIfCount++;
        Console.streaming().dot();
        if (whatIfCount % 75 == 0) Console.streaming().skip();

        final WhatIfOptimizationBuilderImpl whatIfImpl = Objects.cast(builder, WhatIfOptimizationBuilderImpl.class);
        if(whatIfImpl.withProfiledIndex()) throw new UnsupportedOperationException("Error: Used Columns in Database Connection not supported.");

        final String        sql           = whatIfImpl.getSQL();
        final IndexBitSet   usedSet       = whatIfImpl.getUsedSet();
        final IndexBitSet   configuration = whatIfImpl.getConfiguration();
        final List<Index>   indexSet      = newList();
        Double returnVal = supplyValue(
                    explainIndexesCost(usedSet),
                    connection,
                    indexSet,
                    configuration,
                    sql,
                    configuration.cardinality(),
                    new Double[indexSet.size()]
        );

        Console.streaming().info("PostgresIBGWhatIfOptimizer#estimateCost(WhatIfOptimizationBuilder) " +
                "has returned a "
                + (returnVal == null ? "null (which will be set to 0.0)" : returnVal)
                + " workload cost"
        );
        return returnVal == null ? 0.0 : returnVal;
    }

    @Override
    public String toString() {
        return new ToStringBuilder<PostgresIBGWhatIfOptimizer>(this)
               .add("whatIf count", whatIfCount)
               .add("index set", getCachedIndexBitSet())
               .add("candidate index set", getCandidateSet())
            .toString();
    }
}
