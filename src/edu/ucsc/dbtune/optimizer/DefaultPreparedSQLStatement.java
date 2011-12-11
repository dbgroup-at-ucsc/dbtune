package edu.ucsc.dbtune.optimizer;

import java.sql.SQLException;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * A convenience class for a no-op implementation of the {@link PreparedSQLStatement} interface.
 * The {@link PreparedSQLStatement#explain(Configuration)} method simply 
 * calls {{@link Optimizer#explain(SQLStatement,Configuration)}. 
 * 
 * @author alkis
 *
 */
public class DefaultPreparedSQLStatement implements PreparedSQLStatement {

	/**
	 * Constructs a default prepared statement.
	 * @param optimizer The optimizer that created this statement
	 * @param sql The sql statement
	 */
	public DefaultPreparedSQLStatement(Optimizer optimizer, SQLStatement sql)
	{
		this.optimizer 	= optimizer;
		this.sql		= sql;
	}
	
	/**
	 * Constructs a {@link DefaultPreparedSQLStatement} out af another {@link PreparedSQLStatement}
	 * @param other The existing {@link PreparedSQLStatement}
	 */
	public DefaultPreparedSQLStatement(PreparedSQLStatement other) {
		this.optimizer 	= 	other.getOptimizer();
		this.sql		= 	other.getSQLStatement();
	}

	/**
	 * The optimizer that created this statement
	 */
	protected final Optimizer optimizer;
	
	/**
	 * The SQL statement corresponding to the prepared statement
	 */
	protected final SQLStatement sql;

	@Override
	public Optimizer getOptimizer() {
		return optimizer;
	}

	@Override
	public SQLStatement getSQLStatement() {
		return sql;
	}

	@Override
	public ExplainedSQLStatement explain(Configuration configuration)
			throws SQLException {
		return optimizer.explain(sql, configuration);
	}

}
