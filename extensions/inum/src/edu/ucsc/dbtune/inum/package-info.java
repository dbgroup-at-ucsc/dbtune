/**
 * This package contains the implementation of the INdex Usage Model (INUM). The main abstractions 
 * are:
 *  
 * <ul>
 *   <li>{@link edu.ucsc.dbtune.inum.InumSpaceComputation}.</li>
 *   <li>{@link edu.ucsc.dbtune.inum.MatchingStrategy}.</li>
 * </ul>
 *
 * And the extensions done to core's classes:
 *
 * <ul>
 *   <li>{@link edu.ucsc.dbtune.optimizer.InumOptimizer}.</li>
 *   <li>{@link edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement}.</li>
 *   <li>{@link edu.ucsc.dbtune.optimizer.plan.InumPlan}.</li>
 * </ul> 
 *
 * The usage flow is as follows:
 *
 * <ol>
 *   <li>First, inum is configured in {@code config/dbtune.cfg}.</li>
 *   <li>When a statement is optimized through an instance of {@link 
 *   edu.ucsc.dbtune.optimizer.InumOptimizer }, a {@link 
 *   edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement prepared statement} is created. This in 
 *   turns causes the {@link edu.ucsc.dbtune.inum.InumSpaceComputation computation} of the INUM 
 *   space.</li>
 *   <li>When the prepared statement is {@link edu.ucsc.dbtune.optimizer.PreparedSQLStatement} is 
 *   explained, a call to the {@link edu.ucsc.dbtune.inum.MatchingStrategy matching strategy} is 
 *   done and the optimal plan is returned.</li>
 * </ol>
 *
 * @see <a href="http://portal.acm.org/citation.cfm?id=1325974"?>
 *        [1] Efficient use of the query optimizer for automated physical design
 *      </a>
 */
package edu.ucsc.dbtune.inum;
