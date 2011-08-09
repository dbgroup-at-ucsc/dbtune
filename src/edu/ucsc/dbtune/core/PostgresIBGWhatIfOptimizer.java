package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.core.metadata.Index;
import edu.ucsc.dbtune.core.optimizers.WhatIfOptimizationBuilder;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static edu.ucsc.dbtune.connectivity.PGCommands.explainIndexesCost;
import static edu.ucsc.dbtune.spi.core.Functions.supplyValue;
import static edu.ucsc.dbtune.util.Instances.newAtomicInteger;
import static edu.ucsc.dbtune.util.Instances.newBitSet;
import static edu.ucsc.dbtune.util.Instances.newList;

/**
 * A Postgres-specific What-if optimizer.
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
class PostgresIBGWhatIfOptimizer extends AbstractIBGWhatIfOptimizer {
    private final AtomicInteger      whatifCount;
    private final IndexBitSet        cachedBitSet       = newBitSet();
    private final List<Index>      cachedCandidateSet = newList();

    /**
     * construct a {@code Postgres-WhatIfOptimizer} object.
     * @param connection
     *      a database connection
     */
    PostgresIBGWhatIfOptimizer(DatabaseConnection connection){
        super(new PostgresWhatIfOptimizer(connection));
        whatifCount = newAtomicInteger();
    }

    @Override
    public void fixCandidates(Iterable<? extends Index> candidateSet) throws SQLException {
        Checks.checkArgument(isEnabled(), "Error: Database Connection is closed.");
        cachedCandidateSet.clear();
        getCachedIndexBitSet().clear();
        for (Index idx : candidateSet) {
            cachedCandidateSet.add(idx);
            getCachedIndexBitSet().set(idx.getId());
        }
    }

    @Override
    public ExplainInfo explain(String sql) throws SQLException {
        final ExplainInfo result = super.explain(sql);
        incrementWhatIfCount();
        return result;
    }

    @Override
    protected IndexBitSet getCachedIndexBitSet() {
        return cachedBitSet;
    }

    @Override
    protected void updateCachedIndexBitSet(IndexBitSet newOne) {
        cachedBitSet.set(newOne);
    }


    @Override
    public Iterable<Index> getCandidateSet() {
        return cachedCandidateSet;
    }

    @Override
    protected void incrementWhatIfCount() {
        whatifCount.incrementAndGet();
    }

    @Override
    double estimateCost(String sql, Iterable<Index> candidate,
        IndexBitSet configuration, IndexBitSet used) {
	  //List<Index> indexSet = PGIndex.cast(cachedCandidateSet);
      return supplyValue(
          explainIndexesCost(used),
          getConnection(),
          cachedCandidateSet,
          configuration,
          sql,
          configuration.cardinality(),
          new Double[cachedCandidateSet.size()]
      );
    }

    @Override
    protected double estimateCost(WhatIfOptimizationBuilder builder) throws SQLException {
        incrementWhatIfCount();
        Console.streaming().dot();
        if (whatifCount.get() % 75 == 0) Console.streaming().skip();

        final WhatIfOptimizationBuilderImpl whatIfImpl = Objects.cast(builder, WhatIfOptimizationBuilderImpl.class);
        if(whatIfImpl.withProfiledIndex()) throw new UnsupportedOperationException("Error: Used Columns in Database Connection not supported.");

        final String        sql           = whatIfImpl.getSQL();
        final IndexBitSet   usedSet       = whatIfImpl.getUsedSet();
        final IndexBitSet   configuration = whatIfImpl.getConfiguration();
        final List<Index>   indexSet      = newList();
        Double returnVal = supplyValue(
                    explainIndexesCost(usedSet),
                    getConnection(),
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
    public int getWhatIfCount() {
        return whatifCount.get();
    }

    @Override
    public String toString() {
        return new ToStringBuilder<PostgresIBGWhatIfOptimizer>(this)
               .add("whatIf count", getWhatIfCount())
               .add("index set", getCachedIndexBitSet())
               .add("candidate index set", getCandidateSet())
            .toString();
    }
}
