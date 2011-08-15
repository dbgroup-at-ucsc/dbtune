/*
 * ****************************************************************************
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
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */

package edu.ucsc.dbtune;

import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.OptimizerFactory;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.PGOptimizer;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.Objects;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Platform {
    private static final Map<String, OptimizerFactory> AVAILABLE_OPTIMIZERS;
    static {
        Map<String, OptimizerFactory> driverToOptimizer =
                new HashMap<String, OptimizerFactory>() {
                    {
                        put("com.ibm.db2.jcc.DB2Driver", new DB2OptimizerFactory());
                        put("org.postgresql.Driver", new PGOptimizerFactory());
                    }
                };

        AVAILABLE_OPTIMIZERS = Collections.unmodifiableMap(driverToOptimizer);
    }

    /**
     * utility class.
     */
    private Platform(){}

    /**
     * finds the appropriate {@link OptimizerFactory} strategy for a given driver. This method
     * will throw a {@link NullPointerException} if strategy is not found. We rather deal with problem
     * right away (i.e., throwing a NullPointerException) rather than waiting for some side affect along the execution line.
     *
     * @param driver
     *      dbms-driver's fully qualified name.
     * @return
     *      a found {@link OptimizerFactory} pre-instantiated instance.
     * @throws NullPointerException
     *      if strategy is not found.
     */
    public static OptimizerFactory findOptimizerFactory(String driver){
        return Objects.as(Checks.checkNotNull(AVAILABLE_OPTIMIZERS.get(driver)));
    }

    private static class DB2OptimizerFactory extends OptimizerFactory {
        @Override
        public Optimizer newOptimizer(DatabaseConnection connection) {
            try {
                return new DB2Optimizer(connection);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class PGOptimizerFactory extends OptimizerFactory {
        @Override
        public Optimizer newOptimizer(DatabaseConnection connection) {
            try {
                return new PGOptimizer(connection);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
