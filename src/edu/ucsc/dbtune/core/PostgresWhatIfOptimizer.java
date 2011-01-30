package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.core.metadata.PGCommands;
import edu.ucsc.dbtune.core.metadata.PGReifiedTypes;
import edu.ucsc.dbtune.spi.core.Functions;
import edu.ucsc.dbtune.util.*;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author huascar.sanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
class PostgresWhatIfOptimizer extends AbstractWhatIfOptimizer {
    private final DatabaseConnection        connection;
    private final AtomicReference<String>   cachedSQL;
    private final IndexBitSet               indexSet;

    PostgresWhatIfOptimizer(DatabaseConnection connection){
        super();
        this.connection     = connection;
        indexSet            = Instances.newBitSet();
        cachedSQL           = Instances.newAtomicReference();
    }

    @Override
    public ExplainInfo explain(String sql) throws SQLException {
        return explain(sql, Instances.<DBIndex>newList());
    }

    @Override
    public ExplainInfo explain(Iterable<? extends DBIndex> indexes) throws SQLException {
        return explain(cachedSQL.get(), indexes);
    }

    @Override
    protected DatabaseConnection getConnection() {
        return connection;
    }

    @Override
    public ExplainInfo explain(String sql, Iterable<? extends DBIndex> indexes) throws SQLException {
        Checks.checkSQLRelatedState(null != connection && connection.isOpened(), "Connection is closed.");
        Checks.checkArgument(!Strings.isEmpty(sql), "Empty SQL statement");
        updateCachedSQL(sql);
        final PGReifiedTypes.ReifiedPGIndexList indexSet = new PGReifiedTypes.ReifiedPGIndexList(indexes);
        if(getCachedIndexBitSet().isEmpty()) updateCachedIndexBitSet(Instances.newBitSet(indexes));
        final IndexBitSet   cachedIndexBitSet   = getCachedIndexBitSet();
        final Double[]      maintCost           = new Double[indexSet.size()];
        return Functions.supplyValue(
                PGCommands.explainIndexes(),             // postgres's explain index command
                connection,                              // live connection
                indexSet,                                // index set
                cachedIndexBitSet,                       // cached bit set
                sql,                                     // sql statement
                cachedIndexBitSet.cardinality(),         // cardinality
                maintCost                                // maintenance cost
        );
    }

    @Override
    protected IndexBitSet getCachedIndexBitSet() {
        return indexSet;
    }

    @Override
    protected void updateCachedIndexBitSet(IndexBitSet newOne) {
        indexSet.set(newOne);
    }

    @Override
    public String toString() {
        return new ToStringBuilder<PostgresWhatIfOptimizer>(this)
               .add("Active", connection.isOpened())
               .add("CachedSQL", getCachedSQL())
               .add("CachedIndexSet", getCachedIndexBitSet())
               .toString();
    }
}
