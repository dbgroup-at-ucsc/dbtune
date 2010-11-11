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
/**
 * <i>DBTune</i>'s core package. It contains the necessary classes for creating a connection to a dbms.
 * Currently, we only support PostGresql and DB2.
 *
 * <p>The principal public APIs in this package are:
 *
 * <dl>
 * <dt>{@link DatabaseConnectionManager}
 * <dd>The interface that you will use in your implementation classes to tell DBTune
 *     to create connections to a specific dbms.
 *
 * <dt>{@link DatabaseConnection}
 * <dd>The interface you will use in order to perform dbms-specific "operations" --
 *     Additionally, this is the interface that you will use to retrieve dbms-specific index
 *     extraction and what-if optimizer strategies.
 *
 * <dt>{@link DatabaseIndexExtractor}
 * <dd>The object that DBTune uses to perform operations dealing with index extraction.
 *
 * <dt>{@link DatabaseWhatIfOptimizer}
 * <dd>The object that DBTune uses to perform operations dealing with what-if optimizations.
 *
 * </dl>
 */
package edu.ucsc.dbtune.core;