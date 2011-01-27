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

import java.sql.SQLException;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Checks {
    private Checks(){}

    /**
     * checks if a given reference is null. If it is, then throw a
     * {@link NullPointerException null pointer exception}. Return the
     * given reference otherwise.
     * @param reference
     *      an object to be checked.
     * @param <T>
     *      type of object being checked.
     * @return non-null reference.
     */
    public static <T> T checkNotNull(T reference){
        if(reference == null) throw new NullPointerException();
        return reference;
    }

    /**
     * checks if a given reference is null. If it is, then throw a
     * {@link NullPointerException null pointer exception}. Return the
     * given reference otherwise.
     * @param reference
     *      an object to be checked.
     * @param errorMessage
     *      user-specified error message.
     * @param <T>
     *      type of object being checked.
     * @return non-null reference.
     */
    public static <T> T checkNotNull(T reference, Object errorMessage) {
        if (reference == null) {
          throw new NullPointerException(String.valueOf(errorMessage));
        }
        return reference;
    }

    /**
     * checks if a precondition is satisfied. If the result of the check is false, then throw a
     * {@link IllegalArgumentException illegal argument exception}. Do nothing otherwise.
     * @param reference
     *      an object to be checked.
     * @param expression
     *      expression being asserted.
     * @param errorMessage
     *      user-specified error message.
     * @param <T>
     *      type of object being checked.
     * @return  well-behaved reference.
     */
    public static <T> T checkArgument(T reference, boolean expression, Object errorMessage) {
       if(!expression) throw new IllegalArgumentException(String.valueOf(errorMessage));
       return reference;
    }

    /**
     * asserts a given expressions. If the assertion is false, then throw a
     * {@link AssertionError assertion error}. Do nothing otherwise.
     * @param expression
     *      expression being asserted.
     */
    public static void checkAssertion(boolean expression){
        if(!expression) throw new AssertionError();
    }

    /**
     * asserts a given expressions. If the assertion is false, then throw a
     * {@link AssertionError assertion error}. Do nothing otherwise.
     * @param expression
     *      expression being asserted.
     * @param errorMessage
     *      user-specified error message.
     */
    public static void checkAssertion(boolean expression, Object errorMessage){
        if(!expression) throw new AssertionError(String.valueOf(errorMessage));
    }

    /**
     * asserts a given SQL state. If the assertion is false, then throw a
     * {@link SQLException SQL error}. Do nothing otherwise.
     * @param expression
     *      expression being asserted.
     * @throws java.sql.SQLException
     *      throw an error if we have an invalid state.
     */
    public static void checkSQLRelatedState(boolean expression) throws SQLException {
        if(!expression){
            throw new SQLException();
        }
    }

    /**
     * asserts a given SQL state. If the assertion is false, then throw a
     * {@link SQLException SQL error}. Do nothing otherwise.
     * @param expression
     *      expression being asserted.
     * @param errorMessage
     *      user-specified error message.
     * @throws java.sql.SQLException
     *      throw an error if we have an invalid state.
     */
    public static void checkSQLRelatedState(boolean expression, Object errorMessage) throws SQLException {
        if(!expression){
            throw new SQLException(String.valueOf(errorMessage));
        }
    }

    /**
     * checks if a precondition is satisfied. If the result of the check is false, then throw a
     * {@link IllegalArgumentException illegal argument exception}. Do nothing otherwise.
     * @param expression
     *      expression being asserted.
     */
    public static void checkArgument(boolean expression) {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * checks if a precondition is satisfied. If the result of the check is false, then throw a
     * {@link IllegalArgumentException illegal argument exception}. Do nothing otherwise.
     * @param expression
     *      expression being asserted.
     * @param errorMessage
     *      user-specified error message.
     */
    public static void checkArgument(boolean expression, Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

}
