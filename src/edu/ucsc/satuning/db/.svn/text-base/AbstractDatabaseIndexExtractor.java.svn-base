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
package edu.ucsc.satuning.db;

import edu.ucsc.satuning.util.BitSet;
import edu.ucsc.satuning.util.ToStringBuilder;

import java.util.concurrent.atomic.AtomicBoolean;

import static edu.ucsc.satuning.util.Util.newTrueBoolean;

/**
 * This class provides a skeletal implementation of the {@link DatabaseIndexExtractor}
 * interface to minimize the effort required to implement this interface.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
abstract class AbstractDatabaseIndexExtractor <I extends DBIndex<I>> implements DatabaseIndexExtractor <I> {
    private final AtomicBoolean  enabled;
    protected AbstractDatabaseIndexExtractor(){
        enabled = newTrueBoolean();
    }

    @Override
    public void disable(){
        enabled.set(false);
    }

    /**
     * @return a current candidate set after calling {@link #fixCandidates(Iterable)} method.
     */
    public abstract Iterable<I> getCandidateSet();

    /**
     * @return the current cached bit set.
     */
    public abstract BitSet getCachedBitSet();

    /**
     * @return {@code true} if we can use this extractor, {@code false} otherwise.
     */
    protected boolean isEnabled(){
        return enabled.get();
    }

    @Override
    public String toString() {
        return new ToStringBuilder<AbstractDatabaseIndexExtractor<?>>(this)
               .add("enabled?", isEnabled())
               .toString();
    }
}
