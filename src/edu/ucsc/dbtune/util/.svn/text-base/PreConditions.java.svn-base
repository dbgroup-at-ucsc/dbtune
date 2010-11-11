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
public class PreConditions {
    private PreConditions(){}

    public static <T> T checkNotNull(T reference){
        if(reference == null) throw new NullPointerException();
        return reference;
    }


    public static <T> T checkNotNull(T reference, Object errorMessage) {
        if (reference == null) {
          throw new NullPointerException(String.valueOf(errorMessage));
        }
        return reference;
    }

    public static void checkAssertion(boolean expression){
        if(!expression) throw new AssertionError();
    }

    public static void checkAssertion(boolean expression, Object errorMessage){
        if(!expression) throw new AssertionError(String.valueOf(errorMessage));
    }

    public static void checkSQLRelatedState(boolean expression) throws SQLException {
        if(!expression){
            throw new SQLException();
        }
    }

    public static void checkSQLRelatedState(boolean expression, Object errorMessage) throws SQLException {
        if(!expression){
            throw new SQLException(String.valueOf(errorMessage));
        }
    }

    public static void checkArgument(boolean expression) {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    public static void checkArgument(boolean expression, Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }
}
