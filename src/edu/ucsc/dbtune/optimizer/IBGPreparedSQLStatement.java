package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.ibg.IBGCoveringNodeFinder;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.ibg.IBGCoveringNodeFinder.FindResult;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.ConfigurationBitSet;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.sql.SQLException;

/**
 * Prepared statements that are produced by the {@link IBGOptimizer}.
 *
 * @see IBGOptimizer
 * @author Karl Schnaitter
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 * @author Neoklis Polyzotis
 */
public class IBGPreparedSQLStatement extends DefaultPreparedSQLStatement
{
    /** The {@link IndexBenefitGraph} used by this prepared statement */
    private IndexBenefitGraph ibg;

    /** The universe of indexes from which actual explains will occur */
    private Configuration universe;

    /** used to find nodes in the ibg */
    private static final IBGCoveringNodeFinder NODE_FINDER = new IBGCoveringNodeFinder();

    /**
     * Constructcs a prepared statement.
     *  
     * @param optimizer 
     *      The optimizer that created this statement
     * @param sql
     *      The SQL statement that corresponds to this prepared statement
     * 
     */
    public IBGPreparedSQLStatement(IBGOptimizer optimizer, SQLStatement sql)
    {
        this(optimizer,sql,null,null);
    }
    
    /**
     * Constructs a prepared statement.
     *
     * @param optimizer
     *     The optimizer that created this statement
     * @param sql
     *     The SQL statement that corresponds to this prepared statement
     * @param ibg
     *     IBG that this new statement will use to execute what-if optimization calls
     * @param universe
     *      The set of all indexes of interest
     */    
    public IBGPreparedSQLStatement(
            IBGOptimizer         optimizer,
            SQLStatement         sql,
            IndexBenefitGraph    ibg,
            Configuration        universe)
    {
        super(optimizer,sql);
        this.ibg        = ibg;
        this.universe   = universe;
    }

    /**
     * copy constructor
     */
    public IBGPreparedSQLStatement(IBGPreparedSQLStatement other)
    {
        super(other);

        this.ibg = other.ibg;
        this.universe	= other.universe;
    }

    /**
     * @return an {@link IndexBenefitGraph} of this query.
     */
    public IndexBenefitGraph getIndexBenefitGraph()
    {
        return ibg;
    }


    @Override
    public Optimizer getOptimizer()
    {
        return optimizer;
    }

    @Override
    public SQLStatement getSQLStatement()
    {
        return sql;
    }

    public Configuration getUniverse()
    {
        return universe;
    }

    /**
     * Uses the IBG to obtain a new {@link ExplainedSQLStatement}.
     *
     * @param configuration
     *      the configuration considered to estimate the cost of the new statement. This can (or 
     *      not) be the same as {@link #getConfiguration}.
     * @return
     *      a new statement
     * @throws SQLException
     *      if it's not possible to do what-if optimization on the given configuration
     */
    @Override
    public ExplainedSQLStatement explain(Configuration configuration) throws SQLException
    {
        if (ibg==null) {
            // Time to build the IBG
            int oldOptimizationCount = optimizer.getWhatIfCount();
            ibg = ((IBGOptimizer)optimizer).buildIBG(sql, configuration);
            int optimizationCount = optimizer.getWhatIfCount() - oldOptimizationCount;
            
            universe = new Configuration(configuration);
            IBGNode rootNode = ibg.rootNode();
            
            return new ExplainedSQLStatement(
                    getSQLStatement(), rootNode.cost(), optimizer, universe,
                    new ConfigurationBitSet(universe,rootNode.getUsedIndexes()),optimizationCount);
        }
        
        if (!getUniverse().contains(configuration)) {
            throw new SQLException("Configuration " + configuration +
                    " not contained in statement's" + getUniverse());
        }

        if (configuration.isEmpty()) {
        	double cost = getIndexBenefitGraph().emptyCost();
        	return new ExplainedSQLStatement( getSQLStatement(), cost, optimizer, configuration, new Configuration("Empty"), 0);
        } 

        ConfigurationBitSet configurationBitSet = null;

        if (configuration instanceof ConfigurationBitSet ) {
            configurationBitSet = Objects.as(configuration);
        } else {
            configurationBitSet = new ConfigurationBitSet(configuration);
        }
        
        FindResult result = NODE_FINDER.find(getIndexBenefitGraph(),configurationBitSet);

        if (result == null) // This is the case where the IBG is incomplete
            throw new SQLException("IBG construction has not completed yet");
            
        return new ExplainedSQLStatement(
                getSQLStatement(),
                result.cost,
                optimizer,
                configuration,
                result.usedConfiguration, 
                0);
    }
}
