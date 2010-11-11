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

import edu.ucsc.dbtune.util.BitSet;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface BenefitFunction<I> {
    /**
     * Applies the function to an index object of type {@code I} and to an index configuration,
     * resulting in an object of type {@code double}, which an the benefit value of the index
     * object given an index configuration.
     *
     * @param arg the index object.
     * @param m the index configuration.
     * @return the benefit value of the index object given an index configuration.
     */
    double apply(I arg, BitSet m);
}
