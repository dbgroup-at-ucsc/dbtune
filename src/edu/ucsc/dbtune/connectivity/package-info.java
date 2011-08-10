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
/**
 * Contains the necessary classes for creating a connection to a DBMS.
 * <dl>
 * <dt>{@link edu.ucsc.dbtune.connectivity.ConnectionManager}
 * <dd>The interface that you will use in your implementation classes to tell DBTune
 *     to create connections to a specific dbms.
 *
 * <dt>{@link edu.ucsc.dbtune.connectivity.DatabaseConnection}
 * <dd>The interface you will use in order to perform dbms-specific "operations" --
 *     Additionally, this is the interface that you will use to retrieve dbms-specific index
 *     extraction and what-if optimizer strategies.
 * </dl>
 *
 */
@Generated(value={})
package edu.ucsc.dbtune.connectivity;

import javax.annotation.Generated;
