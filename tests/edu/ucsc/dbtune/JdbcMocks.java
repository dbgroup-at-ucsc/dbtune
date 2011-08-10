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
package edu.ucsc.dbtune;

import edu.ucsc.dbtune.util.Instances;
import edu.ucsc.satuning.util.Objects;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class JdbcMocks {
    private JdbcMocks(){}

    public static Statement makeMockStatement(boolean next, boolean withCounterForGetInt, Connection connection){
        final MockStatement statement = new MockStatement(new MockResultSet(next, false, withCounterForGetInt));
        statement.register(connection);
        return statement;
    }

    public static Statement makeMockStatementSwitchOffOne(boolean next, boolean withCounterForGetInt, Connection connection){
        final MockStatement statement = new MockStatement(new MockResultSet(next, true, withCounterForGetInt));
        statement.register(connection);
        return statement;
    }

    public static ResultSet makeResultSet(){
        return new MockResultSet(true, false, true);
    }

    public static PreparedStatement makeMockPreparedStatementSwitchOffOne(boolean next, boolean withCounterForGetInt, Connection connection){
        final MockPreparedStatement statement = new MockPreparedStatement(new MockResultSet(next, true, withCounterForGetInt));
        statement.register(connection);
        return statement;
    }

    public static PreparedStatement makeMockPreparedStatement(boolean next, boolean withCounterForGetInt, Connection connection){
        final MockPreparedStatement statement = new MockPreparedStatement(new MockResultSet(next, false, withCounterForGetInt));
        statement.register(connection);
        return statement;
    }

    static class MockStatement implements Statement {
        protected final ResultSet resultSet;
        private Connection connection;
        MockStatement(ResultSet resultSet){
            this.connection = null;
            this.resultSet  = resultSet;
        }

        void register(Connection connection){
            this.connection = connection;
        }

        private static void print(String sql){
            System.out.println(sql + "");
        }

        private static UnsupportedOperationException notSupportedLalalala() throws UnsupportedOperationException {
            throw new UnsupportedOperationException("method not supported - this is a mock object, remember?");
        }

        @Override
        public ResultSet executeQuery(String sql) throws SQLException {
            print(sql);
            return resultSet;
        }

        @Override
        public int executeUpdate(String sql) throws SQLException {
            print(sql);
            return 0;
        }

        @Override
        public void close() throws SQLException {
            System.out.println("closed!!");
        }

        @Override
        public int getMaxFieldSize() throws SQLException {
            throw notSupportedLalalala();
        }

        @Override
        public void setMaxFieldSize(int max) throws SQLException {
        }

        @Override
        public int getMaxRows() throws SQLException {
            throw notSupportedLalalala();
        }

        @Override
        public void setMaxRows(int max) throws SQLException {
        }

        @Override
        public void setEscapeProcessing(boolean enable) throws SQLException {
        }

        @Override
        public int getQueryTimeout() throws SQLException {
            throw notSupportedLalalala();
        }

        @Override
        public void setQueryTimeout(int seconds) throws SQLException {
        }

        @Override
        public void cancel() throws SQLException {
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            throw notSupportedLalalala();
        }

        @Override
        public void clearWarnings() throws SQLException {
        }

        @Override
        public void setCursorName(String name) throws SQLException {
        }

        @Override
        public boolean execute(String sql) throws SQLException {
            print(sql);
            return true;
        }

        @Override
        public ResultSet getResultSet() throws SQLException {
            return resultSet;
        }

        @Override
        public int getUpdateCount() throws SQLException {
            return -1;
        }

        @Override
        public boolean getMoreResults() throws SQLException {
            return false;
        }

        @Override
        public void setFetchDirection(int direction) throws SQLException {
            //todo
        }

        @Override
        public int getFetchDirection() throws SQLException {
            throw notSupportedLalalala();
        }

        @Override
        public void setFetchSize(int rows) throws SQLException {
        }

        @Override
        public int getFetchSize() throws SQLException {
            throw notSupportedLalalala();
        }

        @Override
        public int getResultSetConcurrency() throws SQLException {
            throw notSupportedLalalala();
        }

        @Override
        public int getResultSetType() throws SQLException {
            throw notSupportedLalalala();
        }

        @Override
        public void addBatch(String sql) throws SQLException {
        }

        @Override
        public void clearBatch() throws SQLException {
        }

        @Override
        public int[] executeBatch() throws SQLException {
            return new int[0];
        }

        @Override
        public Connection getConnection() throws SQLException {
            return connection;
        }

        @Override
        public boolean getMoreResults(int current) throws SQLException {
            return false;
        }

        @Override
        public ResultSet getGeneratedKeys() throws SQLException {
            return resultSet;
        }

        @Override
        public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
            print(sql);
            return 0;
        }

        @Override
        public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
            print(sql);
            return 0;
        }

        @Override
        public int executeUpdate(String sql, String[] columnNames) throws SQLException {
            print(sql);
            return 0;
        }

        @Override
        public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
            print(sql);
            return true;
        }

        @Override
        public boolean execute(String sql, int[] columnIndexes) throws SQLException {
            return false;  //todo
        }

        @Override
        public boolean execute(String sql, String[] columnNames) throws SQLException {
            print(sql);
            return true;
        }

        @Override
        public int getResultSetHoldability() throws SQLException {
            throw notSupportedLalalala();
        }

        @Override
        public boolean isClosed() throws SQLException {
            return false;
        }

        @Override
        public void setPoolable(boolean poolable) throws SQLException {
        }

        @Override
        public boolean isPoolable() throws SQLException {
            return false;
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw notSupportedLalalala();
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }
    }

    static class MockPreparedStatement extends MockStatement implements PreparedStatement {
        final AtomicReference<String> sql = Instances.newAtomicReference();
        MockPreparedStatement(ResultSet resultSet){
            super(resultSet);
        }

        void setSQL(String sql){
            this.sql.set(sql);
        }

        @Override
        public ResultSet executeQuery() throws SQLException {
            return resultSet;
        }

        @Override
        public int executeUpdate() throws SQLException {
            return 0;
        }

        @Override
        public void setNull(int parameterIndex, int sqlType) throws SQLException {
            //todo
        }

        @Override
        public void setBoolean(int parameterIndex, boolean x) throws SQLException {
            //todo
        }

        @Override
        public void setByte(int parameterIndex, byte x) throws SQLException {
            //todo
        }

        @Override
        public void setShort(int parameterIndex, short x) throws SQLException {
            //todo
        }

        @Override
        public void setInt(int parameterIndex, int x) throws SQLException {
            //todo
        }

        @Override
        public void setLong(int parameterIndex, long x) throws SQLException {
            //todo
        }

        @Override
        public void setFloat(int parameterIndex, float x) throws SQLException {
            //todo
        }

        @Override
        public void setDouble(int parameterIndex, double x) throws SQLException {
            //todo
        }

        @Override
        public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
            //todo
        }

        @Override
        public void setString(int parameterIndex, String x) throws SQLException {
            //todo
        }

        @Override
        public void setBytes(int parameterIndex, byte[] x) throws SQLException {
            //todo
        }

        @Override
        public void setDate(int parameterIndex, Date x) throws SQLException {
            //todo
        }

        @Override
        public void setTime(int parameterIndex, Time x) throws SQLException {
            //todo
        }

        @Override
        public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
            //todo
        }

        @Override
        public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
            //todo
        }

        @Override
        public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
            //todo
        }

        @Override
        public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
            //todo
        }

        @Override
        public void clearParameters() throws SQLException {
            //todo
        }

        @Override
        public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
            //todo
        }

        @Override
        public void setObject(int parameterIndex, Object x) throws SQLException {
            //todo
        }

        @Override
        public boolean execute() throws SQLException {
            System.out.println(sql.get());
            return false;
        }

        @Override
        public void addBatch() throws SQLException {
            //todo
        }

        @Override
        public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
            //todo
        }

        @Override
        public void setRef(int parameterIndex, Ref x) throws SQLException {
            //todo
        }

        @Override
        public void setBlob(int parameterIndex, Blob x) throws SQLException {
            //todo
        }

        @Override
        public void setClob(int parameterIndex, Clob x) throws SQLException {
            //todo
        }

        @Override
        public void setArray(int parameterIndex, Array x) throws SQLException {
            //todo
        }

        @Override
        public ResultSetMetaData getMetaData() throws SQLException {
            return null;  //todo
        }

        @Override
        public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
            //todo
        }

        @Override
        public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
            //todo
        }

        @Override
        public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
            //todo
        }

        @Override
        public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
            //todo
        }

        @Override
        public void setURL(int parameterIndex, URL x) throws SQLException {
            //todo
        }

        @Override
        public ParameterMetaData getParameterMetaData() throws SQLException {
            return null;  //todo
        }

        @Override
        public void setRowId(int parameterIndex, RowId x) throws SQLException {
            //todo
        }

        @Override
        public void setNString(int parameterIndex, String value) throws SQLException {
            //todo
        }

        @Override
        public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
            //todo
        }

        @Override
        public void setNClob(int parameterIndex, NClob value) throws SQLException {
            //todo
        }

        @Override
        public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
            //todo
        }

        @Override
        public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
            //todo
        }

        @Override
        public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
            //todo
        }

        @Override
        public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
            //todo
        }

        @Override
        public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
            //todo
        }

        @Override
        public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
            //todo
        }

        @Override
        public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
            //todo
        }

        @Override
        public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
            //todo
        }

        @Override
        public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
            //todo
        }

        @Override
        public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
            //todo
        }

        @Override
        public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
            //todo
        }

        @Override
        public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
            //todo
        }

        @Override
        public void setClob(int parameterIndex, Reader reader) throws SQLException {
            //todo
        }

        @Override
        public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
            //todo
        }

        @Override
        public void setNClob(int parameterIndex, Reader reader) throws SQLException {
            //todo
        }
    }

    public static class MockConnection implements Connection {
        private Statement         statement;
        private PreparedStatement preparedStatement;

        public MockConnection(){
            this.statement         = null;
            this.preparedStatement = null;
        }

        public void register(Statement statement, PreparedStatement preparedStatement){
            this.statement = statement;
            this.preparedStatement = preparedStatement;
        }

        @Override
        public Statement createStatement() throws SQLException {
            return statement;
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            final MockPreparedStatement ps = Objects.as(preparedStatement);
            ps.setSQL(sql);
            return ps;
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            return null;  //todo
        }

        @Override
        public String nativeSQL(String sql) throws SQLException {
            return null;  //todo
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            //todo
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return false;  //todo
        }

        @Override
        public void commit() throws SQLException {
            System.out.println("committed!!");
        }

        @Override
        public void rollback() throws SQLException {
            System.out.println("rolled back!!");
        }

        @Override
        public void close() throws SQLException {
            System.out.println("closed!!");
        }

        @Override
        public boolean isClosed() throws SQLException {
            return false;  //todo
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return null;  //todo
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            //todo
        }

        @Override
        public boolean isReadOnly() throws SQLException {
            return false;  //todo
        }

        @Override
        public void setCatalog(String catalog) throws SQLException {
            //todo
        }

        @Override
        public String getCatalog() throws SQLException {
            return null;  //todo
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            //todo
        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return 0;  //todo
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return null;  //todo
        }

        @Override
        public void clearWarnings() throws SQLException {
            //todo
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return statement;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return preparedStatement;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return null;  //todo
        }

        @Override
        public Map<String, Class<?>> getTypeMap() throws SQLException {
            return null;  //todo
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            //todo
        }

        @Override
        public void setHoldability(int holdability) throws SQLException {
            //todo
        }

        @Override
        public int getHoldability() throws SQLException {
            return 0;  //todo
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            return null;  //todo
        }

        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            return null;  //todo
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            //todo
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            //todo
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return statement;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return preparedStatement;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return null;  //todo
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return preparedStatement;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            return preparedStatement;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            return preparedStatement;
        }

        @Override
        public Clob createClob() throws SQLException {
            return null;  //todo
        }

        @Override
        public Blob createBlob() throws SQLException {
            return null;  //todo
        }

        @Override
        public NClob createNClob() throws SQLException {
            return null;  //todo
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            return null;  //todo
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            return false;  //todo
        }

        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            //todo
        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            //todo
        }

        @Override
        public String getClientInfo(String name) throws SQLException {
            return null;  //todo
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            return new Properties();
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return null;  //todo
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return null;  //todo
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return null;  //todo
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;  //todo
        }
    }

    static class MockResultSet implements ResultSet {
        private final boolean next;
        private final AtomicInteger counterforGetInt;
        private Double costcost = 0.0;
        private final boolean withCounter;
        private final AtomicInteger nextCounter;
        private final AtomicBoolean oneNoOneYes;
        private final AtomicInteger oneNoOneYesCounter;

        MockResultSet(boolean next, boolean oneNoOneYes, boolean withCounterforGetInt){
            this.next        = next;
            counterforGetInt = new AtomicInteger(0);
            costcost         = 0.0;
            withCounter      = withCounterforGetInt;
            nextCounter      = new AtomicInteger(0);
            this.oneNoOneYes = new AtomicBoolean(oneNoOneYes);
            this.oneNoOneYesCounter = new AtomicInteger(0);
        }

        @Override
        public boolean next() throws SQLException {
            if(oneNoOneYes.get()){
                if(oneNoOneYesCounter.get() == 1) return false;
                oneNoOneYesCounter.incrementAndGet();
                return oneNoOneYes.get();
            } else {
                if(nextCounter.get() == 3){
                    nextCounter.set(0);
                    return false;
                }
                nextCounter.incrementAndGet();
                return next;
            }
        }

        @Override
        public void close() throws SQLException {
            System.out.println("result set got closed!!!");
        }

        @Override
        public boolean wasNull() throws SQLException {
            return false;  //todo
        }

        @Override
        public String getString(int columnIndex) throws SQLException {
            if(columnIndex > 1){
                if(columnIndex == 6){
                    return "U";
                }

                if(columnIndex == 8){
                    return "Y";
                }

                if(columnIndex == 9){
                    return "REG";
                }
                return "+A-B+C";
            }
            return 1 == columnIndex ? DBTuneInstances.SCHEMA_NAME : DBTuneInstances.TABLE_NAME;
        }

        @Override
        public boolean getBoolean(int columnIndex) throws SQLException {
            return false;  //todo
        }

        @Override
        public byte getByte(int columnIndex) throws SQLException {
            return 0;  //todo
        }

        @Override
        public short getShort(int columnIndex) throws SQLException {
            return 0;  //todo
        }

        @Override
        public int getInt(int columnIndex) throws SQLException {
            if (withCounter) {
                counterforGetInt.incrementAndGet();
                if(counterforGetInt.get() == 2){
                    counterforGetInt.set(1);
                }
            }
            return counterforGetInt.get();
        }

        @Override
        public long getLong(int columnIndex) throws SQLException {
            return 0;  //todo
        }

        @Override
        public float getFloat(int columnIndex) throws SQLException {
            return 0;  //todo
        }

        @Override
        public double getDouble(int columnIndex) throws SQLException {
            return ++costcost;
        }

        @Override
        public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
            return null;  //todo
        }

        @Override
        public byte[] getBytes(int columnIndex) throws SQLException {
            return new byte[0];  //todo
        }

        @Override
        public Date getDate(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public Time getTime(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public Timestamp getTimestamp(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public InputStream getAsciiStream(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public InputStream getUnicodeStream(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public InputStream getBinaryStream(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public String getString(String columnLabel) throws SQLException {
            if(columnLabel.equalsIgnoreCase("index_overhead")) return "0=1.1 1=2.0 2=3.0";
            if(columnLabel.equalsIgnoreCase("desc")) return "Y N Y N N Y";
            if(columnLabel.equalsIgnoreCase("reloid")) return Integer.valueOf(123456).toString();
            if(columnLabel.equalsIgnoreCase("sync")) return "Y";
            if(columnLabel.equalsIgnoreCase("atts")) return "1 2 3 4 5 6";
            if(columnLabel.equalsIgnoreCase("qcost")) return Double.valueOf(1.0).toString();
            if(columnLabel.equalsIgnoreCase("indexes")) return Integer.valueOf(31).toString();
            if(columnLabel.equalsIgnoreCase("create_cost")) return Double.valueOf(1.0).toString();
            if(columnLabel.equalsIgnoreCase("megabytes")) return Double.valueOf(1.0).toString();
            return DBTuneInstances.SCHEMA_NAME;
        }

        @Override
        public boolean getBoolean(String columnLabel) throws SQLException {
            return false;  //todo
        }

        @Override
        public byte getByte(String columnLabel) throws SQLException {
            return 0;  //todo
        }

        @Override
        public short getShort(String columnLabel) throws SQLException {
            return 0;  //todo
        }

        @Override
        public int getInt(String columnLabel) throws SQLException {
            return 0;  //todo
        }

        @Override
        public long getLong(String columnLabel) throws SQLException {
            return 0;  //todo
        }

        @Override
        public float getFloat(String columnLabel) throws SQLException {
            return 0;  //todo
        }

        @Override
        public double getDouble(String columnLabel) throws SQLException {
            return 0;  //todo
        }

        @Override
        public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
            return null;  //todo
        }

        @Override
        public byte[] getBytes(String columnLabel) throws SQLException {
            return new byte[0];  //todo
        }

        @Override
        public Date getDate(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public Time getTime(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public Timestamp getTimestamp(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public InputStream getAsciiStream(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public InputStream getUnicodeStream(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public InputStream getBinaryStream(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return null;  //todo
        }

        @Override
        public void clearWarnings() throws SQLException {
            //todo
        }

        @Override
        public String getCursorName() throws SQLException {
            return null;  //todo
        }

        @Override
        public ResultSetMetaData getMetaData() throws SQLException {
            return null;  //todo
        }

        @Override
        public Object getObject(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public Object getObject(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public int findColumn(String columnLabel) throws SQLException {
            return 0;  //todo
        }

        @Override
        public Reader getCharacterStream(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public Reader getCharacterStream(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public boolean isBeforeFirst() throws SQLException {
            return false;  //todo
        }

        @Override
        public boolean isAfterLast() throws SQLException {
            return false;  //todo
        }

        @Override
        public boolean isFirst() throws SQLException {
            return false;  //todo
        }

        @Override
        public boolean isLast() throws SQLException {
            return false;  //todo
        }

        @Override
        public void beforeFirst() throws SQLException {
            //todo
        }

        @Override
        public void afterLast() throws SQLException {
            //todo
        }

        @Override
        public boolean first() throws SQLException {
            return false;  //todo
        }

        @Override
        public boolean last() throws SQLException {
            return false;  //todo
        }

        @Override
        public int getRow() throws SQLException {
            return 0;  //todo
        }

        @Override
        public boolean absolute(int row) throws SQLException {
            return false;  //todo
        }

        @Override
        public boolean relative(int rows) throws SQLException {
            return false;  //todo
        }

        @Override
        public boolean previous() throws SQLException {
            return false;  //todo
        }

        @Override
        public void setFetchDirection(int direction) throws SQLException {
            //todo
        }

        @Override
        public int getFetchDirection() throws SQLException {
            return 0;  //todo
        }

        @Override
        public void setFetchSize(int rows) throws SQLException {
            //todo
        }

        @Override
        public int getFetchSize() throws SQLException {
            return 0;  //todo
        }

        @Override
        public int getType() throws SQLException {
            return 0;  //todo
        }

        @Override
        public int getConcurrency() throws SQLException {
            return 0;  //todo
        }

        @Override
        public boolean rowUpdated() throws SQLException {
            return false;  //todo
        }

        @Override
        public boolean rowInserted() throws SQLException {
            return false;  //todo
        }

        @Override
        public boolean rowDeleted() throws SQLException {
            return false;  //todo
        }

        @Override
        public void updateNull(int columnIndex) throws SQLException {
            //todo
        }

        @Override
        public void updateBoolean(int columnIndex, boolean x) throws SQLException {
            //todo
        }

        @Override
        public void updateByte(int columnIndex, byte x) throws SQLException {
            //todo
        }

        @Override
        public void updateShort(int columnIndex, short x) throws SQLException {
            //todo
        }

        @Override
        public void updateInt(int columnIndex, int x) throws SQLException {
            //todo
        }

        @Override
        public void updateLong(int columnIndex, long x) throws SQLException {
            //todo
        }

        @Override
        public void updateFloat(int columnIndex, float x) throws SQLException {
            //todo
        }

        @Override
        public void updateDouble(int columnIndex, double x) throws SQLException {
            //todo
        }

        @Override
        public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
            //todo
        }

        @Override
        public void updateString(int columnIndex, String x) throws SQLException {
            //todo
        }

        @Override
        public void updateBytes(int columnIndex, byte[] x) throws SQLException {
            //todo
        }

        @Override
        public void updateDate(int columnIndex, Date x) throws SQLException {
            //todo
        }

        @Override
        public void updateTime(int columnIndex, Time x) throws SQLException {
            //todo
        }

        @Override
        public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
            //todo
        }

        @Override
        public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
            //todo
        }

        @Override
        public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
            //todo
        }

        @Override
        public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
            //todo
        }

        @Override
        public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
            //todo
        }

        @Override
        public void updateObject(int columnIndex, Object x) throws SQLException {
            //todo
        }

        @Override
        public void updateNull(String columnLabel) throws SQLException {
            //todo
        }

        @Override
        public void updateBoolean(String columnLabel, boolean x) throws SQLException {
            //todo
        }

        @Override
        public void updateByte(String columnLabel, byte x) throws SQLException {
            //todo
        }

        @Override
        public void updateShort(String columnLabel, short x) throws SQLException {
            //todo
        }

        @Override
        public void updateInt(String columnLabel, int x) throws SQLException {
            //todo
        }

        @Override
        public void updateLong(String columnLabel, long x) throws SQLException {
            //todo
        }

        @Override
        public void updateFloat(String columnLabel, float x) throws SQLException {
            //todo
        }

        @Override
        public void updateDouble(String columnLabel, double x) throws SQLException {
            //todo
        }

        @Override
        public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
            //todo
        }

        @Override
        public void updateString(String columnLabel, String x) throws SQLException {
            //todo
        }

        @Override
        public void updateBytes(String columnLabel, byte[] x) throws SQLException {
            //todo
        }

        @Override
        public void updateDate(String columnLabel, Date x) throws SQLException {
            //todo
        }

        @Override
        public void updateTime(String columnLabel, Time x) throws SQLException {
            //todo
        }

        @Override
        public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
            //todo
        }

        @Override
        public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
            //todo
        }

        @Override
        public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
            //todo
        }

        @Override
        public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
            //todo
        }

        @Override
        public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
            //todo
        }

        @Override
        public void updateObject(String columnLabel, Object x) throws SQLException {
            //todo
        }

        @Override
        public void insertRow() throws SQLException {
            //todo
        }

        @Override
        public void updateRow() throws SQLException {
            //todo
        }

        @Override
        public void deleteRow() throws SQLException {
            //todo
        }

        @Override
        public void refreshRow() throws SQLException {
            //todo
        }

        @Override
        public void cancelRowUpdates() throws SQLException {
            //todo
        }

        @Override
        public void moveToInsertRow() throws SQLException {
            //todo
        }

        @Override
        public void moveToCurrentRow() throws SQLException {
            //todo
        }

        @Override
        public Statement getStatement() throws SQLException {
            return null;  //todo
        }

        @Override
        public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
            return null;  //todo
        }

        @Override
        public Ref getRef(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public Blob getBlob(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public Clob getClob(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public Array getArray(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
            return null;  //todo
        }

        @Override
        public Ref getRef(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public Blob getBlob(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public Clob getClob(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public Array getArray(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public Date getDate(int columnIndex, Calendar cal) throws SQLException {
            return null;  //todo
        }

        @Override
        public Date getDate(String columnLabel, Calendar cal) throws SQLException {
            return null;  //todo
        }

        @Override
        public Time getTime(int columnIndex, Calendar cal) throws SQLException {
            return null;  //todo
        }

        @Override
        public Time getTime(String columnLabel, Calendar cal) throws SQLException {
            return null;  //todo
        }

        @Override
        public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
            return null;  //todo
        }

        @Override
        public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
            return null;  //todo
        }

        @Override
        public URL getURL(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public URL getURL(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public void updateRef(int columnIndex, Ref x) throws SQLException {
            //todo
        }

        @Override
        public void updateRef(String columnLabel, Ref x) throws SQLException {
            //todo
        }

        @Override
        public void updateBlob(int columnIndex, Blob x) throws SQLException {
            //todo
        }

        @Override
        public void updateBlob(String columnLabel, Blob x) throws SQLException {
            //todo
        }

        @Override
        public void updateClob(int columnIndex, Clob x) throws SQLException {
            //todo
        }

        @Override
        public void updateClob(String columnLabel, Clob x) throws SQLException {
            //todo
        }

        @Override
        public void updateArray(int columnIndex, Array x) throws SQLException {
            //todo
        }

        @Override
        public void updateArray(String columnLabel, Array x) throws SQLException {
            //todo
        }

        @Override
        public RowId getRowId(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public RowId getRowId(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public void updateRowId(int columnIndex, RowId x) throws SQLException {
            //todo
        }

        @Override
        public void updateRowId(String columnLabel, RowId x) throws SQLException {
            //todo
        }

        @Override
        public int getHoldability() throws SQLException {
            return 0;  //todo
        }

        @Override
        public boolean isClosed() throws SQLException {
            return false;  //todo
        }

        @Override
        public void updateNString(int columnIndex, String nString) throws SQLException {
            //todo
        }

        @Override
        public void updateNString(String columnLabel, String nString) throws SQLException {
            //todo
        }

        @Override
        public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
            //todo
        }

        @Override
        public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
            //todo
        }

        @Override
        public NClob getNClob(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public NClob getNClob(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public SQLXML getSQLXML(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public SQLXML getSQLXML(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
            //todo
        }

        @Override
        public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
            //todo
        }

        @Override
        public String getNString(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public String getNString(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public Reader getNCharacterStream(int columnIndex) throws SQLException {
            return null;  //todo
        }

        @Override
        public Reader getNCharacterStream(String columnLabel) throws SQLException {
            return null;  //todo
        }

        @Override
        public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
            //todo
        }

        @Override
        public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
            //todo
        }

        @Override
        public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
            //todo
        }

        @Override
        public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
            //todo
        }

        @Override
        public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
            //todo
        }

        @Override
        public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
            //todo
        }

        @Override
        public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
            //todo
        }

        @Override
        public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
            //todo
        }

        @Override
        public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
            //todo
        }

        @Override
        public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
            //todo
        }

        @Override
        public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
            //todo
        }

        @Override
        public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
            //todo
        }

        @Override
        public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
            //todo
        }

        @Override
        public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
            //todo
        }

        @Override
        public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
            //todo
        }

        @Override
        public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
            //todo
        }

        @Override
        public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
            //todo
        }

        @Override
        public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
            //todo
        }

        @Override
        public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
            //todo
        }

        @Override
        public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
            //todo
        }

        @Override
        public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
            //todo
        }

        @Override
        public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
            //todo
        }

        @Override
        public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
            //todo
        }

        @Override
        public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
            //todo
        }

        @Override
        public void updateClob(int columnIndex, Reader reader) throws SQLException {
            //todo
        }

        @Override
        public void updateClob(String columnLabel, Reader reader) throws SQLException {
            //todo
        }

        @Override
        public void updateNClob(int columnIndex, Reader reader) throws SQLException {
            //todo
        }

        @Override
        public void updateNClob(String columnLabel, Reader reader) throws SQLException {
            //todo
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return null;  //todo
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;  //todo
        }
    }
}
