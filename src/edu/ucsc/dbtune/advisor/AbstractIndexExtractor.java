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

package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.util.ToStringBuilder;

import java.util.concurrent.atomic.AtomicBoolean;

import static edu.ucsc.dbtune.util.Instances.newTrueBoolean;

/**
 * This class provides a skeletal implementation of the {@link IndexExtractor}
 * interface to minimize the effort required to implement this interface.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public abstract class AbstractIndexExtractor implements IndexExtractor {
    private final AtomicBoolean  enabled;
    protected AbstractIndexExtractor(){
        enabled = newTrueBoolean();
    }

    /**
     * @return {@code true} if we can use this extractor, {@code false} otherwise.
     */
    protected boolean isEnabled(){
        return enabled.get();
    }

    @Override
    public String toString() {
        return new ToStringBuilder<AbstractIndexExtractor>(this)
               .add("enabled?", isEnabled())
               .toString();
    }
}
