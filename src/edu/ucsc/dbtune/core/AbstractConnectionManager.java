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

package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.ToStringBuilder;

/**
 * This class provides a skeletal implementation of the {@link ConnectionManager}
 * interface to minimize the effort required to implement this interface.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
abstract class AbstractConnectionManager
        implements ConnectionManager {

	private final String username;
	private final String password;
	private final String database;

	protected AbstractConnectionManager(
            String username,
            String password,
            String database
    ) {
		this.username = Checks.checkNotNull(username);
		this.password = Checks.checkNotNull(password);
		this.database = Checks.checkNotNull(database);
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

    @Override
    public String getDatabaseName() {
        return database;
    }


    @Override
    public String toString() {
        return new ToStringBuilder<AbstractConnectionManager>(this)
                .add("username", getUsername())
                .add("password", "......hidden.........")
                .add("database", getDatabaseName())
                .toString();
    }
}