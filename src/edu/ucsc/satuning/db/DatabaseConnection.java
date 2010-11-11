package edu.ucsc.satuning.db;

/**
 * A connection to a specific database.  {@code DatbaseConnection} objects are obtained by using
 * {@link DatabaseConnectionManager#connect()}.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 * @see DatabaseSession
 * @see DatabaseConnectionManager
 * @param <I> the type of {@link DBIndex}.
 */
public interface DatabaseConnection<I extends DBIndex<I>> extends DatabaseSession {
	/**
	 * gets the instance of the connection manager that created this connection.
	 * @return
     *      the {@link DatabaseConnectionManager connection manager} instance that created
     *      this connection.
     * @throws NullPointerException
     *      it will throw a null pointer exception if the connection mananger is null.
     *      this is a normal side effect when the connection has been closed.
	 */
    DatabaseConnectionManager<I> getConnectionManager();

    /**
     * gets the instance of a database index extractor created for this connection.
     * @return
     *      the {@link DatabaseIndexExtractor index extractor} instance created
     *      for this connection.
     * @throws NullPointerException
     *      it will throw a null pointer exception if the index extractor is null.
     *      this is a normal side effect when the connection was already closed.
     */
    DatabaseIndexExtractor<I> getIndexExtractor();

    /**
     * gets the instance of what-if optmizer created for this connection.
     * @return
     *     the {@link DatabaseWhatIfOptimizer what-if optimizer} instance created for
     *     this connection.
     * @throws NullPointerException
     *      it will throw a null pointer exception if the optimizer is null.
     *      this is a normal side effect when the connection was already closed.
     */
    DatabaseWhatIfOptimizer<I> getWhatIfOptimizer();

    /**
     * install both a new {@link DatabaseIndexExtractor} strategy, and a new
     * {@link DatabaseWhatIfOptimizer} strategy after a {@code connection} object
     * was fully created.
     *
     * @param indexExtractor
     *      a new {@link DatabaseIndexExtractor} instance.
     * @param whatIfOptimizer
     *      a new {@link DatabaseWhatIfOptimizer} instance.
     */
    void install(DatabaseIndexExtractor<I> indexExtractor, DatabaseWhatIfOptimizer<I> whatIfOptimizer);

}
