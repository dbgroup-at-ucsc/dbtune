package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Instances;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Skeletal implementation of {@link WhatIfOptimizer} type.
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
abstract class AbstractWhatIfOptimizer implements WhatIfOptimizer {
    protected final AtomicReference<String> cachedSQL;
    protected AbstractWhatIfOptimizer(){
        cachedSQL = Instances.newAtomicReference();
    }

    @Override
    public ExplainInfo explain(String sql) throws SQLException {
        return explain(sql, Instances.<DBIndex>newList());
    }

    @Override
    public ExplainInfo explain(Iterable<? extends DBIndex> indexes) throws SQLException {
        return explain(getCachedSQL(), indexes);
    }

    /**
     * @return the connection this optimizer was created for.
     */
    protected abstract DatabaseConnection getConnection();

    /**
     * @return a cached set of indexes.
     */
    protected abstract IndexBitSet getCachedIndexBitSet();

    /**
     * @return a cached SQL statement.
     */
    protected String getCachedSQL(){
        return cachedSQL.get();
    }

    /**
     * updates the cached set of indexes before performing a what-if optimization call.
     * @param newOne new set of indexes.
     */
    protected abstract void updateCachedIndexBitSet(IndexBitSet newOne);

    /**
     * update the cached SQL statement before performing a what-if optimization call.
     * @param sql new SQL statement.
     */
    protected void updateCachedSQL(String sql){
        final String currentSQL = getCachedSQL();
        cachedSQL.compareAndSet(currentSQL, sql);
    }
}
