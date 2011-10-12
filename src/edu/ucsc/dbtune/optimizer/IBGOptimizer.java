/* **************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                         *
 *                                                                              *
 *   Licensed under the Apache License, Version 2.0 (the "License");            *
 *   you may not use this file except in compliance with the License.           *
 *   You may obtain a copy of the License at                                    *
 *                                                                              *
 *       http://www.apache.org/licenses/LICENSE-2.0                             *
 *                                                                              *
 *   Unless required by applicable law or agreed to in writing, software        *
 *   distributed under the License is distributed on an "AS IS" BASIS,          *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 *   See the License for the specific language governing permissions and        *
 *   limitations under the License.                                             *
 * **************************************************************************** */
package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.ConfigurationBitSet;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.sql.SQLException;

import static edu.ucsc.dbtune.ibg.IndexBenefitGraphConstructor.construct;

/**
 * Represents a variant of the optimizer concept in the dbtune API that relies on the {@link 
 * IndexBenefitGraph} to optimize statements.
 *
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 * @author Neoklis Polyzotis
 */
public class IBGOptimizer implements Optimizer
{
	
    /**
     * The {@link Optimizer} that the {@link IBGOptimizer} uses for actual what-if calls.
     */
    protected Optimizer delegate;

    /**
     * Constructs an {@code IBGOptimizer}. Relies on the given {@code optimizer} to execute actual 
     * optimization calls.
     *
     * @param optimizer
     *      a DBMS-specific implementation of an {@link Optimizer} type.
     */
    public IBGOptimizer(Optimizer optimizer) {
        this.delegate = optimizer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration recommendIndexes(SQLStatement sql) throws SQLException {
        return delegate.recommendIndexes(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCatalog(Catalog catalog) {
        delegate.setCatalog(catalog);
    }

    /**
     * estimate what-if optimization cost given a single sql statement.
     *
     * @param sql
     *      sql statement
     * @param configuration
     *      an index configuration
     * @return
     *      the prepared statement
     * @throws SQLException
     *      unable to estimate cost due to the stated reasons.
     */
    public ExplainedSQLStatement explain(SQLStatement sql, Configuration configuration)
        throws SQLException
    {
        return delegate.explain(sql,configuration);
    }
    
    /**
     * Build an {@link IndexBenefitGraph} corresponding to a specific {@link SQLStatement} and 
     * a specific {@link Configuration} that represents the universe of indexes.
     * @param sql The statement
     * @param universe The configuration that comprises all indexes of interest
     * @return The index benefit graph
     * @throws SQLException
     */
    IndexBenefitGraph buildIBG(SQLStatement sql, Configuration universe)
    	throws SQLException
    {
        ConfigurationBitSet  bitConf;
        IndexBitSet          bitSet;

        bitSet = new IndexBitSet();

        for(Index idx : universe) {
            bitSet.set(universe.getOrdinalPosition(idx));
        }

        bitConf = new ConfigurationBitSet(universe, bitSet);
        return construct(delegate, sql, bitConf);
    }
    
    @Override
    public PreparedSQLStatement prepareExplain(SQLStatement sql) 
    	throws SQLException
    {	
        return new IBGPreparedSQLStatement(this, sql, null,null);
    }

	@Override
	public ExplainedSQLStatement explain(String sql) throws SQLException {
		return delegate.explain(sql);
	}

	@Override
	public ExplainedSQLStatement explain(SQLStatement sql) throws SQLException {
		return delegate.explain(sql);
	}

	@Override
	public ExplainedSQLStatement explain(String sql, Configuration configuration)
			throws SQLException {
		return delegate.explain(sql,configuration);
	}

	@Override
	public Configuration recommendIndexes(String sql) throws SQLException {
		return delegate.recommendIndexes(sql);
	}

	@Override
	public int getWhatIfCount() {
		return delegate.getWhatIfCount();
	}
}
