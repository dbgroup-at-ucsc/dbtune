/* **************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                         *
 *                                                                              *
 *   Licensed under the Apache License, Version 2.0 (the "License");            *
 *   you may not use this file except in compliance with the License.           *
 *   You may obtain a copy of the License at                                    *
 *                                                                              *
 *       http://www.apache.org/licenses/LICENSE-2.0                             *
 *                                                                              *
 *   Unless required by applicable law or agreed to in writing, software        *
 *   distributed under the License is distributed on an "AS IS" BASIS,          *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 *   See the License for the specific language governing permissions and        *
 *   limitations under the License.                                             *
 * **************************************************************************** */
package edu.ucsc.dbtune.metadata;

import java.sql.Types;

/**
 * Extension to the java.sql.Types class. This will be an enum (issue #70)
 *
 * @author Ivo Jimenez
 */
public class SQLTypes
{
    /** standard */
    public static final int BIGINT                = Types.BIGINT;
    public static final int CHAR                  = Types.CHAR;
    public static final int CHARACTER             = Types.CHAR;
    public static final int CHARACTER_VARYING     = Types.VARCHAR;
    public static final int DATE                  = Types.DATE;
    public static final int DECIMAL               = Types.DECIMAL;
    public static final int DOUBLE                = Types.DOUBLE;
    public static final int FLOAT                 = Types.FLOAT;
    public static final int INTEGER               = Types.INTEGER;
    public static final int INT                   = Types.INTEGER;
    public static final int NUMERIC               = Types.NUMERIC;
    public static final int REAL                  = Types.REAL;
    public static final int SMALLINT              = Types.SMALLINT;
    public static final int TIME                  = Types.TIME;
    public static final int TIMESTAMP             = Types.TIMESTAMP;
    public static final int TINYINT               = Types.TINYINT;
    public static final int VARCHAR               = Types.VARCHAR;

    /** common non-standard */
    public static final int PICTURE               = 632;
    public static final int INTERVAL              = 633;
    public static final int NCHAR                 = 634;
    public static final int LARGEINT              = 635;
    public static final int WCHAR                 = 636;
    public static final int DATETIME              = 637;

    static final int UNKNOWN = -1;

    /**
     * Returns the String representation of a type cod
     *
     * @param type id of the type
     * @return name of the data type
     */
    public static String codeToName( int type )
    {
        if( type == BIGINT )
            return "BIGINT";
        else if( type == CHAR )
            return "CHAR";
        else if( type == VARCHAR )
            return "VARCHAR";
        else if( type == DATE )
            return "DATE";
        else if( type == DECIMAL )
            return "DECIMAL";
        else if( type == DOUBLE )
            return "DOUBLE";
        else if( type == FLOAT )
            return "FLOAT";
        else if( type == INTEGER )
            return "INTEGER";
        else if( type == NUMERIC )
            return "NUMERIC";
        else if( type == REAL )
            return "REAL";
        else if( type == SMALLINT )
            return "SMALLINT";
        else if( type == TIME )
            return "TIME";
        else if( type == TIMESTAMP )
            return "TIMESTAMP";
        else if( type == TINYINT )
            return "TINYINT";
        else if( type == PICTURE )
            return "PICTURE";
        else if( type == NCHAR )
            return "NCHAR";
        else if( type == LARGEINT )
            return "LARGEINT";
        else if( type == INTERVAL )
            return "INTERVAL";
        else if( type == DATETIME )
            return "DATETIME";
        else if( type == UNKNOWN )
            return "UNKNOWN";
        return "NO TYPE";
    }

    /**
     * Returns the size in bytes of any non-literal,non-doble-precision data type.
     *
     * @param type id of the type
     * @return size in bytes of the type; -1 if the type is NUMERIC, DECIMAL, FLOAT, CHARACTER, 
     * VARCHAR, INTERVAL or equivalent.
     */
    public static int getSize( int type )
    {
        if( type == UNKNOWN)
            return -1;
        if( type == TINYINT )
            return 1;
        else if( type == CHAR )
            return 1;
        else if( type == SMALLINT )
            return 2;
        else if( type == INTEGER
                || type == TIME
                || type == DATE
                || type == DATETIME
                || type == REAL )
            return 4;
        else if( type == TIMESTAMP
                || type == BIGINT
                || type == DOUBLE
                || type == LARGEINT )
            return 8;

        // NUMERIC, DECIMAL, FLOAT, CHARACTER, NCHAR, PICTURE, INTERVAL and VARCHAR
        // are set manually, so we can't have a way of knowing its size
        return -1;
    }


    /**
     * Returns the type id of the data type name passed.
     *
     * @param type name of the data type
     * @return integral id corresponding to the name; -1 if name doesn't correspond to any valid 
     * type.
     */
    public static int nameToCode( String type )
    {
        if( type.equals( "BIGINT" ) )
            return BIGINT;
        else if( type.equals( "CHAR" ) || type.equals( "CHARACTER" ) )
            return CHAR;
        else if( type.equals( "DATETIME" ) )
            return DATETIME;
        else if( type.equals( "DATE" ) )
            return DATE;
        else if( type.equals( "DECIMAL" ) )
            return DECIMAL;
        else if( type.equals( "DOUBLE" ) )
            return DOUBLE;
        else if( type.equals( "FLOAT" ) )
            return FLOAT;
        else if( type.equals( "INT" ) ||
                type.equals( "SIGNED INT" ) ||
                type.equals( "UNSIGNED INT" ) ||
                type.equals( "INTEGER" ) )
            return INTEGER;
        else if( type.equals( "NUMERIC" ) ||
                type.equals( "SIGNED NUMERIC" ) ||
                type.equals( "UNSIGNED NUMERIC" ) )
            return NUMERIC;
        else if( type.equals( "REAL" ) )
            return REAL;
        else if( type.equals( "SMALLINT" ) ||
                type.equals( "SIGNED SMALLINT" ) ||
                type.equals( "UNSIGNED SMALLINT" ) )
            return SMALLINT;
        else if( type.equals( "TIME" ) )
            return TIME;
        else if( type.equals( "TIMESTAMP" ) )
            return TIMESTAMP;
        else if( type.equals( "TINYINT" ) ||
                type.equals( "SIGNED TINYINT" ) ||
                type.equals( "UNSIGNED TINYINT" ) )
            return TINYINT;
        else if( type.equals( "VARCHAR" ) )
            return VARCHAR;
        else if( type.equals( "TIME" ) )
            return TIME;
        else if( type.equals( "PICTURE" ) )
            return PICTURE;
        else if( type.equals( "NCHAR" ) )
            return NCHAR;
        else if( type.equals( "LARGEINT" ) ||
                type.equals( "SIGNED LARGEINT" ) ||
                type.equals( "UNSIGNED LARGEINT" ) )
            return LARGEINT;
        else if( type.equals( "INTERVAL" ) )
            return NCHAR;
        else if( type.equals( "UNKNOWN" ) )
            return UNKNOWN;

        return -1;
    }

    /**
     * Whether or not the given type is numeric, that is, type is INTEGER, BIGINT, DECIMAL, DOUBLE, 
     * FLOAT, NUMERIC, REAL, SMALLINT, TINYINT or LARGEINT.
     *
     * @param type type to check
     * @return true if numeric; false otherwise
     */
    public static boolean isNumeric( int type )
    throws Exception
    {
    switch( type )
        {
        case TIME:
        case TIMESTAMP:
        case DATE:
        case PICTURE:
        case INTERVAL:
        case VARCHAR:
        case CHAR:
        case NCHAR:
        case UNKNOWN:
        return false;
        case INTEGER:
        case BIGINT:
        case DECIMAL:
        case DOUBLE:
        case FLOAT:
        case NUMERIC:
        case REAL:
        case SMALLINT:
        case TINYINT:
        case LARGEINT:
        return true;
        default:
        return false;
    }
    }

    /**
     * Whether or not the given type is date-time literal, that is, type is TIME, TIMESTAMP DATE or 
     * DATETIME.
     *
     * @param type type to check
     * @return true if date-time literal; false otherwise
     */
    public static boolean isDateTimeLiteral( int type )
    {
    switch( type )
        {
        case TIME:
        case TIMESTAMP:
        case DATE:
        case DATETIME:
        return true;
        case PICTURE:
        case VARCHAR:
        case CHAR:
        case NCHAR:
        case INTERVAL:
        case INTEGER:
        case BIGINT:
        case DECIMAL:
        case DOUBLE:
        case FLOAT:
        case NUMERIC:
        case REAL:
        case SMALLINT:
        case TINYINT:
        case LARGEINT:
        case UNKNOWN:
        default:
        return false;
    }
    }

    /**
     * Whether or not the given type is string literal, that is, type is VARCHAR, CHAR, NCHAR or 
     * PICTURE.
     *
     * @param type type to check
     * @return true if string literal; false otherwise
     */
    public static boolean isString( int type )
    {
    switch( type )
        {
        case VARCHAR:
        case CHAR:
        case NCHAR:
        case PICTURE:
        return true;
        case TIME:
        case TIMESTAMP:
        case DATE:
        case DATETIME:
        case INTERVAL:
        case INTEGER:
        case BIGINT:
        case DECIMAL:
        case DOUBLE:
        case FLOAT:
        case NUMERIC:
        case REAL:
        case SMALLINT:
        case TINYINT:
        case LARGEINT:
        case UNKNOWN:
        default:
        return false;
    }
    }


    /**
     * Checks if the given type is defined in this class
     * 
     * @param type
     *     type being checked
     * @return
     *     <code>true</code> if the value passed as argument is defined as a type; 
     *     <code>false</code> otherwise
     */
    public static boolean isValidType(int type)
    {
        switch( type )
        {
            case VARCHAR:
            case CHAR:
            case NCHAR:
            case WCHAR:
            case PICTURE:
            case TIME:
            case TIMESTAMP:
            case DATE:
            case DATETIME:
            case INTERVAL:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case NUMERIC:
            case REAL:
            case SMALLINT:
            case TINYINT:
            case LARGEINT:
            case UNKNOWN:
                return true;
            default:
                return false;
        }
    }
}
