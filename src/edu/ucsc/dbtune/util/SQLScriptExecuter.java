/* ************************************************************************** *
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
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied  *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.util;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLScriptExecuter {
    private SQLScriptExecuter(){
        // this is a utility class.
    }

    /**
     * Executes a SQL script on the given connection. The statements are assumed to be all {@code 
     * DDL} statements and are executed using the {@link Statement#executeBatch} method.
     *
     * @param connection
     *     the connection to use for the script
     * @param filename
     *     the path to the SQL script
     * @throws SQLException
     *     if any SQL errors occur
     * @throws IOException
     *     if there is an error reading from the file
     */
    public static void executeInBatch(
            Connection connection,
            String filename,
            int batchSize)
        throws IOException, SQLException
    {
        Statement        statement;
        LineNumberReader lineReader;
        String           command;
        int              counter;
        boolean          autoCommit;

        autoCommit = connection.getAutoCommit();
        lineReader = new LineNumberReader(new BufferedReader(new FileReader(filename)));
        statement  = connection.createStatement();
        counter    = 1;

        connection.setAutoCommit(false);

        while ((command = getNext(lineReader)) != null)
        {
            statement.addBatch(command);

            if (counter % batchSize == 0)
            {
                statement.executeBatch();
                connection.commit();
                statement  = connection.createStatement();
            }
        }

        statement.close();
        connection.setAutoCommit(autoCommit);
    }

    /**
     * Executes a SQL script on the given connection.
     *
     * @param connection
     *     the connection to use for the script
     * @param filename
     *     the path to the SQL script
     * @throws SQLException
     *     if any SQL errors occur
     * @throws IOException
     *     if there is an error reading from the file
     */
    public static void execute(
            Connection connection,
            String filename)
        throws IOException, SQLException
    {
        Statement        statement;
        LineNumberReader lineReader;
        String           command;

        lineReader = new LineNumberReader(new BufferedReader(new FileReader(filename)));

        while ((command = getNext(lineReader)) != null)
        {
            statement = connection.createStatement();

            statement.execute(command);
            statement.close();
        }
    }

    /**
     * Executes a SQL script on the given connection.
     *
     * @param connection
     *     the connection to use for the script
     * @param filename
     *     the path to the SQL script
     * @throws SQLException
     *     if any SQL errors occur
     * @throws IOException
     *     if there is an error reading from the file
     */
    private static String getNext(LineNumberReader lineReader)
        throws IOException
    {
        StringBuffer command;
        String       line;
        String       trimmedLine;
        boolean      isInvalidLine;

        command = new StringBuffer();

        while ((line = lineReader.readLine()) != null)
        {
            trimmedLine = line.trim();

            isInvalidLine =
                trimmedLine.startsWith("--") ||
                trimmedLine.length() < 1     ||
                trimmedLine.startsWith("//") ;

            if (isInvalidLine)
            {
                // Do nothing
            }
            else if ( trimmedLine.endsWith( ";" ) )
            {
                command.append(line.substring(0, line.lastIndexOf(";"))).append(" ");
                return command.toString();
            }
            else
            {
                command.append(line);
                command.append(" ");
            }
        }

        return null;
    }
}
