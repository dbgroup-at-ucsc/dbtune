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

package edu.ucsc.dbtune.core.metadata;

import edu.ucsc.dbtune.core.DatabaseColumn;
import edu.ucsc.dbtune.util.Objects;

import java.io.Serializable;

public class DB2Column implements DatabaseColumn, Serializable {
	private static final long serialVersionUID = 1L;
	private String name;
	
	DB2Column(String n) {
		this.name = n;
	}
	
	@Override
	public boolean equals(Object other) {
        return other instanceof DB2Column
               && getName().equals(((DB2Column) other).getName());
    }

    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        return 34 * Objects.hashCode(getName());
    }

    @Override
	public String toString() {
		return getName();
	}

}
