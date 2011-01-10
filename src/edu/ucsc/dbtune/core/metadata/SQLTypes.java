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
 * ****************************************************************************
 */

package edu.ucsc.dbtune.core.metadata;

import java.sql.Types;

/**
 * Extension to the java.sql.Types class
 *
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
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

    /**
     * Returns the String representation of a type cod
     *
     * @param type id of the type
     * @return name of the data type
     */
    public static String codeToName( int type )
    {
	if( type == SQLTypes.BIGINT )
	{
	    return "BIGINT";
	}
	else if( type == SQLTypes.CHAR )
        {
	    return "CHAR";
	}
        else if( type == SQLTypes.VARCHAR )
        {
	    return "VARCHAR";
	}
        else if( type == SQLTypes.DATE )
        {
	    return "DATE";
	}
        else if( type == SQLTypes.DECIMAL )
        {
	    return "DECIMAL";
	}
        else if( type == SQLTypes.DOUBLE ){
	    return "DOUBLE";
	}
        else if( type == SQLTypes.FLOAT )
        {
	    return "FLOAT";
	}
        else if( type == SQLTypes.INTEGER )
        {
	    return "INTEGER";
	}
        else if( type == SQLTypes.NUMERIC )
        {
	    return "NUMERIC";
	}
        else if( type == SQLTypes.REAL )
        {
	    return "REAL";
	}
        else if( type == SQLTypes.SMALLINT )
        {
	    return "SMALLINT";
	}
        else if( type == SQLTypes.TIME )
        {
	    return "TIME";
	}
        else if( type == SQLTypes.TIMESTAMP )
        {
	    return "TIMESTAMP";
	}
        else if( type == SQLTypes.TINYINT )
        {
	    return "TINYINT";
	}
        else if( type == SQLTypes.PICTURE )
        {
	    return "PICTURE";
	}
        else if( type == SQLTypes.NCHAR )
        {
	    return "NCHAR";
	}
        else if( type == SQLTypes.LARGEINT )
        {
	    return "LARGEINT";
	}
        else if( type == SQLTypes.INTERVAL )
        {
	    return "INTERVAL";
	}
        else if( type == SQLTypes.DATETIME )
        {
	    return "DATETIME";
	}
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
	if( type == SQLTypes.TINYINT )
        {
	    return 1;
	}
        else if( type == SQLTypes.CHAR )
        {
	    return 1;
	}
        else if( type == SQLTypes.SMALLINT )
        {
	    return 2;
	}
        else if( type == SQLTypes.INTEGER
		|| type == SQLTypes.TIME
		|| type == SQLTypes.DATE
		|| type == SQLTypes.DATETIME
		|| type == SQLTypes.REAL )
	{
	    return 4;
	}
        else if( type == SQLTypes.TIMESTAMP
		|| type == SQLTypes.BIGINT
		|| type == SQLTypes.DOUBLE
		|| type == SQLTypes.LARGEINT )
	{
	    return 8;
	}

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
        {
	    return SQLTypes.BIGINT;
	}
        else if( type.equals( "CHAR" ) || type.equals( "CHARACTER" ) )
        {
	    return SQLTypes.CHAR;
	}
        else if( type.equals( "DATETIME" ) )
        {
	    return SQLTypes.DATETIME;
	}
        else if( type.equals( "DATE" ) )
        {
	    return SQLTypes.DATE;
	}
        else if( type.equals( "DECIMAL" ) )
        {
	    return SQLTypes.DECIMAL;
	}
        else if( type.equals( "DOUBLE" ) )
        {
	    return SQLTypes.DOUBLE;
	}
        else if( type.equals( "FLOAT" ) )
        {
	    return SQLTypes.FLOAT;
	}
        else if( type.equals( "INT" ) ||
		 type.equals( "SIGNED INT" ) ||
		 type.equals( "UNSIGNED INT" ) ||
		 type.equals( "INTEGER" ) )
	{
	    return SQLTypes.INTEGER;
	}
        else if( type.equals( "NUMERIC" ) ||
		 type.equals( "SIGNED NUMERIC" ) ||
		 type.equals( "UNSIGNED NUMERIC" ) )
	{
	    return SQLTypes.NUMERIC;
	}
        else if( type.equals( "REAL" ) )
        {
	    return SQLTypes.REAL;
	}
        else if( type.equals( "SMALLINT" ) ||
		 type.equals( "SIGNED SMALLINT" ) ||
		 type.equals( "UNSIGNED SMALLINT" ) )
	{
	    return SQLTypes.SMALLINT;
	}
        else if( type.equals( "TIME" ) )
        {
	    return SQLTypes.TIME;
	}
        else if( type.equals( "TIMESTAMP" ) )
        {
	    return SQLTypes.TIMESTAMP;
	}
        else if( type.equals( "TINYINT" ) ||
		 type.equals( "SIGNED TINYINT" ) ||
		 type.equals( "UNSIGNED TINYINT" ) )
	{
	    return SQLTypes.TINYINT;
	}
        else if( type.equals( "VARCHAR" ) )
        {
	    return SQLTypes.VARCHAR;
	}
        else if( type.equals( "TIME" ) )
        {
	    return SQLTypes.TIME;
	}
        else if( type.equals( "PICTURE" ) )
        {
	    return SQLTypes.PICTURE;
	}
        else if( type.equals( "NCHAR" ) )
        {
	    return SQLTypes.NCHAR;
	}
        else if( type.equals( "LARGEINT" ) ||
		 type.equals( "SIGNED LARGEINT" ) ||
		 type.equals( "UNSIGNED LARGEINT" ) )
	{
	    return SQLTypes.LARGEINT;
	}
        else if( type.equals( "INTERVAL" ) )
        {
	    return SQLTypes.NCHAR;
	}

	return -1;
    }

    public static void main(String[] args)
    {
	System.out.println( "BIGINT: " + SQLTypes.BIGINT );
	System.out.println( "CHAR: " + SQLTypes.CHAR );
	System.out.println( "VARCHAR: " + SQLTypes.VARCHAR );
	System.out.println( "DATE: " + SQLTypes.DATE );
	System.out.println( "DECIMAL: " + SQLTypes.DECIMAL );
	System.out.println( "DOUBLE: " + SQLTypes.DOUBLE );
	System.out.println( "FLOAT: " + SQLTypes.FLOAT );
	System.out.println( "INTEGER: " + SQLTypes.INTEGER );
	System.out.println( "NUMERIC: " + SQLTypes.NUMERIC );
	System.out.println( "SMALLINT: " + SQLTypes.SMALLINT );
	System.out.println( "TIME: " + SQLTypes.TIME );
	System.out.println( "TIMESTAMP: " + SQLTypes.TIMESTAMP );
	System.out.println( "TINYINT: " + SQLTypes.TINYINT );
	System.out.println( "PICTURE: " + SQLTypes.PICTURE );
	System.out.println( "NCHAR: " + SQLTypes.NCHAR );
	System.out.println( "LARGEINT: " + SQLTypes.LARGEINT );
	System.out.println( "INTERVAL: " + SQLTypes.INTERVAL );
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
	    case SQLTypes.TIME:
	    case SQLTypes.TIMESTAMP:
	    case SQLTypes.DATE:
	    case SQLTypes.PICTURE:
	    case SQLTypes.INTERVAL:
	    case SQLTypes.VARCHAR:
	    case SQLTypes.CHAR:
	    case SQLTypes.NCHAR:
		return false;
	    case SQLTypes.INTEGER:
	    case SQLTypes.BIGINT:
	    case SQLTypes.DECIMAL:
	    case SQLTypes.DOUBLE:
	    case SQLTypes.FLOAT:
	    case SQLTypes.NUMERIC:
	    case SQLTypes.REAL:
	    case SQLTypes.SMALLINT:
	    case SQLTypes.TINYINT:
	    case SQLTypes.LARGEINT:
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
	    case SQLTypes.TIME:
	    case SQLTypes.TIMESTAMP:
	    case SQLTypes.DATE:
	    case SQLTypes.DATETIME:
		return true;
	    case SQLTypes.PICTURE:
	    case SQLTypes.VARCHAR:
	    case SQLTypes.CHAR:
	    case SQLTypes.NCHAR:
	    case SQLTypes.INTERVAL:
	    case SQLTypes.INTEGER:
	    case SQLTypes.BIGINT:
	    case SQLTypes.DECIMAL:
	    case SQLTypes.DOUBLE:
	    case SQLTypes.FLOAT:
	    case SQLTypes.NUMERIC:
	    case SQLTypes.REAL:
	    case SQLTypes.SMALLINT:
	    case SQLTypes.TINYINT:
	    case SQLTypes.LARGEINT:
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
	    case SQLTypes.VARCHAR:
	    case SQLTypes.CHAR:
	    case SQLTypes.NCHAR:
	    case SQLTypes.PICTURE:
		return true;
	    case SQLTypes.TIME:
	    case SQLTypes.TIMESTAMP:
	    case SQLTypes.DATE:
	    case SQLTypes.DATETIME:
	    case SQLTypes.INTERVAL:
	    case SQLTypes.INTEGER:
	    case SQLTypes.BIGINT:
	    case SQLTypes.DECIMAL:
	    case SQLTypes.DOUBLE:
	    case SQLTypes.FLOAT:
	    case SQLTypes.NUMERIC:
	    case SQLTypes.REAL:
	    case SQLTypes.SMALLINT:
	    case SQLTypes.TINYINT:
	    case SQLTypes.LARGEINT:
	    default:
		return false;
	}
    }
}
