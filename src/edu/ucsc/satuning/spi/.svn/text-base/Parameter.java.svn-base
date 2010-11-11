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
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface Parameter {
    /**
     * Gets the name that was specified for this parameter. This is an
     * arbitrary name.
     *
     *
     * @return the name that was specified for this parameter.
     */
    String getParameterName();

    /**
     * Gets the value of this parameter.
     * <strong>Importan Note</strong>: for cases when you have <em>one</em> interface (InterfaceO)
     * and two implementations (e.g., {@code Impl1}, {@code Impl2}) and then you invoke this method (e.g.,
     * getParameterValue(InteferfaceO) to get {@code Impl2}. Then you are in trouble, since what you
     * will get is {@code Impl1} as the implementation of this parameter will return the first implementation
     * of {@code InterfaceO}. How to solve this? Assuming you know the implementation of such interface
     * in advance, then use that implementation class when calling this method.
     *
     * @param type
     *      type of value to be returned.
     * @return the value of this parameter.
     * @throws NullPointerException
     *      unable to find the appropriate type.
     */
    <T> T getParameterValue(Class<T> type);


    /**
     * Sets the value of this parameter.
     * it supports the case where you have multiple types per value. e.g.
     * boolean.class (primitive) and Boolean.class (object) pointing to true/false.
     *
     * @param value the value of this parameter
     * @param types
     *      an array of keys pointing to the same value.
     */
    <T> void setParameterValue(T value, Class<T>... types);
}
