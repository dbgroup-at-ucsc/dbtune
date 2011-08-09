package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.core.optimizers.OptimizerFactory;
import edu.ucsc.dbtune.core.optimizers.Optimizer;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.Strings;

import java.util.NoSuchElementException;

import static edu.ucsc.dbtune.core.Platform.findIndexExtractorFactory;
import static edu.ucsc.dbtune.core.Platform.findWhatIfOptimizerFactory;
import static edu.ucsc.dbtune.core.Platform.findOptimizerFactory;

/**
 * Enumerates the supported DBMS systems.
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public enum DatabaseSystem {
    /**
     * Postgres DBMS system
     */
    POSTGRES("org.postgresql.Driver"),

    /**
     * DB2 DBMS system
     */
    DB2("com.ibm.db2.jcc.DB2Driver");

    private final String                 driver;
    private final WhatIfOptimizerFactory wiof;
    private final IndexExtractorFactory  ief;
    private final OptimizerFactory       of;

    DatabaseSystem(String driver){
        this.driver = driver;
        wiof        = findWhatIfOptimizerFactory(driver);
        ief         = findIndexExtractorFactory(driver);
        of          = findOptimizerFactory(driver);
    }

    /**
     * @param connection
     *      database connection
     * @return a simplified what-if optimizer.
     */
    public WhatIfOptimizer getSimplifiedWhatIfOptimizer(DatabaseConnection connection){
        return wiof.newWhatIfOptimizer(Checks.checkNotNull(connection));
    }

    /**
     * @param connection
     *      database connection
     * @return
     *      an optimizer.
     */
    public Optimizer geOptimizer(DatabaseConnection connection){
        return of.newOptimizer(connection);
    }

    /**
     * @param connection
     *      database connection
     * @return
     *      an IBG-specific what-if optimizer.
     */
    public IBGWhatIfOptimizer getIBGWhatIfOptimizer(DatabaseConnection connection){
        return wiof.newIBGWhatIfOptimizer(Checks.checkNotNull(connection));
    }

    /**
     * @param connection
     *      database connection
     * @return an instance of the index extractor.
     */
    public IndexExtractor getIndexExtractor(DatabaseConnection connection){
        return ief.newIndexExtractor("/home/karlsch/sqllib/bin/db2advis", Checks.checkNotNull(connection));
    }


    /**
     * checks whether a given {@link DatabaseSystem} is equal to {@link DatabaseSystem this}. e.g.,
     * if you want to know if this {@link DatabaseSystem} is {@link DatabaseSystem#POSTGRES}, then all
     * you need to do is perform the following check {@code #isSame(DatabaseSystem.POSTGRES)}.
     * @param toThat
     *      the {@link DatabaseSystem} being checked.
     * @return
     *      {@code true} if the {@link DatabaseSystem} is equal to {@link DatabaseSystem this}. {@code false}
     *      otherwise.
     */
    public boolean isSame(DatabaseSystem toThat){
        return this == toThat;
    }

    /**
     * Determine the used DBMS given the JDBC driver's fully qualified name.
     * @param driver
     *      JDBC driver's fully qualified name.
     * @return
     *      the inferred {@link DatabaseSystem} object.
     */
    public static DatabaseSystem fromQualifiedName(String driver){
        for(DatabaseSystem each : values()){
            if(Strings.same(each.driver, driver)) return each;
        }

        throw new NoSuchElementException("Error: Element not found.");
    }


    @Override
    public String toString() {
        return String.format("%s", isSame(POSTGRES) ? "Postgres" : "DB2");
    }


}
