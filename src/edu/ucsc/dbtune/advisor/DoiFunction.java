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
package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.metadata.Index;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface DoiFunction {
    /**
     * Applies the function to two index objects of type {@code I}, resulting in an object
     * of type {@code double}, which the {@code doi value} of those two index objects.
     *
     * @param a first index object.
     * @param b second index object.
     * @return the doi value of two index objects.
     */
    double apply(Index a, Index b);
}
