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

package edu.ucsc.dbtune.util;

import java.util.BitSet;

public class DefaultBitSet extends BitSet {
	private static final long serialVersionUID = 1L;
	
	private static final BitSet t = new BitSet();
	
	public DefaultBitSet() {
		super();
	}

    @Override
	public DefaultBitSet clone() {
		return (DefaultBitSet) super.clone();
	}
	
	public final void set(DefaultBitSet other) {
		clear();
		or(other);
	}
	
	// probably better in average case
	public final boolean subsetOf(DefaultBitSet b) {
		synchronized (t) {
			t.clear();
			t.or(this);
			t.and(b);
			return (t.equals(this));
		}
	}
    
}
