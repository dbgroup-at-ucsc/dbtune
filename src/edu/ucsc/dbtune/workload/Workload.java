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
package edu.ucsc.dbtune.workload;

import edu.ucsc.dbtune.core.optimizers.SQLStatement;

import java.util.Iterator;
import java.util.List;
import java.lang.Iterable;

/**
 * Represents a workload.
 */
public class Workload implements Iterable<SQLStatement>
{
	List<SQLStatement> sqls;

	/**
	 * Creates a workload containing 
	 */
	public Workload(String workloadFile) {
		// XXX: create one SQLStatement per statement in the file
		throw new RuntimeException("not implemented yet");
	}

	/**
	 * Iterates
	 */
	public Iterator<SQLStatement> iterator() {
		return sqls.iterator();
	}

	// possible useful methods:
	//  *  SQLStatement get(int i)      // return the ith statement contained in the workload
	//
	//  *  SQLStatement get(String sql) // return the statement corresponding to the given sql
	//                                  // string. This would require string matching stuff, or
	//                                  // even having to do some query processing (parse, rewrite 
	//                                  // views, query flattening, etc) in order to compare queries
}
