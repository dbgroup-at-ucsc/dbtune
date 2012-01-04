package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.inum.Inum;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.StopWatch;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.HashSet;

import com.google.common.base.Preconditions;

/**
 * default implementation of {@link InumWhatIfOptimizer} type.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumWhatIfOptimizerImpl implements InumWhatIfOptimizer
{
    private static final Set<Index> EMPTY_CONFIGURATION = new HashSet<Index>();

    private final Inum inum;

    /**
     * construct a new {@code InumWhatIfOptimizer} object.
     * @param catalog
     *    a container of schema objects.
     * @param connection
     *    a live database connection to postgres
     */
    public InumWhatIfOptimizerImpl(Catalog catalog, Connection connection)
    {
        this(Inum.newInumInstance(catalog, Preconditions.checkNotNull(connection)));
    }

    /**
     * construct a new {@code InumWhatIfOptimizer} object.
     * @param inum
     *    a new instance of INUM.
     */
    public InumWhatIfOptimizerImpl(Inum inum)
    {
        this.inum = inum;
    }

    /**
     * shut down INUM. This method may be called when the main component of the dbtune API
     * request a closing of their {@link Connection} object. Once this db connection is
     * closed, there is not need to keep INUM 'on' too.
     */
    public void endInum()
    {
        if (getInum().isStarted()) getInum().end();
    }


    @Override
    public double estimateCost(String query) throws SQLException
    {
        if (getInum().isEnded()) { startInum(); }
        return estimateCost(query, EMPTY_CONFIGURATION);
    }

    @Override
    public double estimateCost(String query, Set<Index> hypotheticalIndexes)
        throws SQLException
    {
        if (getInum().isEnded()) { startInum(); }

        return getInum().estimateCost(query, hypotheticalIndexes);
    }

    /**
     * @return the assigned {@link Inum} object.
     */
    public Inum getInum()
    {
        return inum;
    }

    /**
     * start INUM.
     * @throws SQLException
     *    if unable to build inum space.
     */
    public void startInum() throws SQLException
    {
        final StopWatch inumStarting = new StopWatch();
        getInum().start();
        inumStarting.resetAndLog("inum starting took ");
    }

    @Override public String toString()
    {
        return String.format("Inum status = %s" + getInum().toString());
    }
}
