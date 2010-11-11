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
package edu.ucsc.satuning.spi;

/**
 * Either a transformation from one object to another, a closure, or a
 * builder.
 *
 * @param <R> the type of the function output
 * @param <E> the type of the exception to throw in case of errors.
 * 
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface Command<R, E extends Exception> {
    /**
     * Applies the function to an object of type {@code Parameter}, resulting in an object
     * of type {@code R}.  Note that types {@code Parameter} and {@code R} may or may not
     * be the same.
     *
     * @param input
     *      the source object
     * @return
     *      the resulting object
     * @throws E
     *      an unexpected error has occurred.
     */
    R apply(Parameter input) throws E;

    /**
     * Indicates whether some other object is equal to this {@code Command}.
     * This method can return {@code true} <i>only</i> if the specified object is
     * also a {@code Function} and, for every input object {@code o}, it returns
     * exactly the same value.  Thus, {@code command1.equals(command2)} implies
     * that either {@code command1.apply(o)} and {@code command2.apply(o)} are
     * both null, or {@code command1.apply(o).equals(command2.apply(o))}.
     *
     * <p>Note that it is always safe <em>not</em> to override
     * {@link Object#equals}.
     */
    boolean equals(Object obj);
}
