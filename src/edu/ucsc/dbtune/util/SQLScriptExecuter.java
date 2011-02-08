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

package edu.ucsc.dbtune.util;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLScriptExecuter
{
    /**
     * Runs a SQL script (read in using the Reader parameter) using the
     * connection passed in
     * 
     * @param conn
     *     the connection to use for the script
     * @param filename
     *     the path to the SQL script
     * @throws SQLException
     *     if any SQL errors occur
     * @throws IOException
     *     if there is an error reading from the file
     */
    public static void execute(Connection conn, String filename)
        throws IOException, SQLException
    {
        Statement        statement;
        BufferedReader   reader;
        LineNumberReader lineReader;
        StringBuffer     command;
        String           line;
        String           trimmedLine;
        boolean          isInvalidLine;

        conn.setAutoCommit(true);

        reader  = new BufferedReader(new FileReader(filename));
        command = null;

        try
        {
            lineReader = new LineNumberReader(reader);
            line       = lineReader.readLine();

            while ( line != null )
            {
                if (command == null)
                {
                    command = new StringBuffer();
                }

                trimmedLine   = line.trim();
                isInvalidLine = trimmedLine.startsWith("--") ||
                                trimmedLine.length() < 1     ||
                                trimmedLine.startsWith("//") ;
                
                if (isInvalidLine)
                {
                    // Do nothing
                }
                else if ( trimmedLine.endsWith( ";" ) )
                {
                    statement = conn.createStatement();

                    command.append(line.substring( 0, line.lastIndexOf(";") ) + " ");
                    statement.execute(command.toString());

                    command = new StringBuffer();

                    statement.close();
                }
                else
                {
                    command.append(line);
                    command.append(" ");
                }

                line = lineReader.readLine();
            }
        }
        catch (SQLException e)
        {
            e.fillInStackTrace();
            throw e;
        }
        catch (IOException e)
        {
            e.fillInStackTrace();
            throw e;
        }
    }
}