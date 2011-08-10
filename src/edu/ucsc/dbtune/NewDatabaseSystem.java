/* ************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied  *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune;

import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.metadata.extraction.GenericJDBCExtractor;
import edu.ucsc.dbtune.metadata.extraction.PGExtractor;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.advisor.Advisor;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.util.Strings;

import java.sql.Connection;
import java.util.List;

import static edu.ucsc.dbtune.connectivity.JdbcConnectionManager.makeDatabaseConnectionManager;

/**
 * This class provides a hub for most of the operations that a user can execute through the DBTune API.
 *
 * @author Ivo Jimenez
 */
public class NewDatabaseSystem
{
    private Catalog              catalog;
    private Environment          environment;
    private DatabaseConnection   connection;
    private GenericJDBCExtractor extractor;

    /**
     * Creates a DBSystem class based on the Environment singleton.
     */
    public NewDatabaseSystem() throws Exception {
        environment = Environment.getInstance();
        connection  = makeDatabaseConnectionManager(environment.getAll()).connect();
        extractor   = Strings.contains(environment.getJDBCDriver(), "postgresql")
                        ? new PGExtractor()
                        : new GenericJDBCExtractor();
        catalog     = extractor.extract(connection);
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public Connection getConnection() {
        return connection.getJdbcConnection();
    }

    public Optimizer getOptimizer() throws Exception {
        // XXX: check the environment to see what type of optimizer the user wants
        throw new RuntimeException("not implemented yet");
    }

    public Advisor getAdvisor() throws Exception {
        // XXX: check the environment to see what type of advisor the user wants
        throw new RuntimeException("not implemented yet");
    }

    public Index createIndex(List<Column> cols, List<Boolean> descending, int type) {
        throw new RuntimeException("not implemented yet");
    }
}
