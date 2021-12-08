/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

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
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLType;
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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.resource.transaction.spi.DdlTransactionIsolator;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tool.schema.internal.exec.ImprovedExtractionContextImpl;
import org.hibernate.tool.schema.internal.exec.JdbcContext;

import static org.hibernate.reactive.util.impl.CompletionStages.logSqlException;
import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;

public class ReactiveImprovedExtractionContextImpl extends ImprovedExtractionContextImpl {

	private final ReactiveConnectionPool service;

	public ReactiveImprovedExtractionContextImpl(
			ServiceRegistry registry,
			SqlStringGenerationContext sqlStringGenerationContext,
			DatabaseObjectAccess databaseObjectAccess) {
		super(
				registry,
				registry.getService( JdbcEnvironment.class ),
				sqlStringGenerationContext,
				NoopDdlTransactionIsolator.INSTANCE,
				databaseObjectAccess
		);
		service = registry.getService( ReactiveConnectionPool.class );
	}

	@Override
	public <T> T getQueryResults(
			String queryString,
			Object[] positionalParameters,
			ResultSetProcessor<T> resultSetProcessor) throws SQLException {

		final CompletionStage<ReactiveConnection> connectionStage = service.getConnection();

		try (final ResultSet resultSet = getQueryResultSet( queryString, positionalParameters, connectionStage )) {
			return resultSetProcessor.process( resultSet );
		}
		finally {
			// We start closing the connection but we don't care about the result
			connectionStage.whenComplete( (c, e) -> c.close() );
		}
	}

	private ResultSet getQueryResultSet(
			String queryString,
			Object[] positionalParameters,
			CompletionStage<ReactiveConnection> connectionStage) {
		final Object[] parametersToUse = positionalParameters != null ? positionalParameters : new Object[0];
		final Parameters parametersDialectSpecific = Parameters.instance(
				getJdbcEnvironment().getDialect()
		);
		final String queryToUse = parametersDialectSpecific.process( queryString, parametersToUse.length );
		return connectionStage.thenCompose( c -> c.selectJdbcOutsideTransaction( queryToUse, parametersToUse ) )
				.handle( (resultSet, err) -> {
					logSqlException( err, () -> "could not execute query ", queryToUse );
					return returnOrRethrow( err, resultSet );
				} )
				.thenApply(ResultSetWorkaround::new)
				.toCompletableFuture()
				.join();
	}

	private static class NoopDdlTransactionIsolator implements DdlTransactionIsolator {
		static final NoopDdlTransactionIsolator INSTANCE = new NoopDdlTransactionIsolator();

		private NoopDdlTransactionIsolator() {
		}

		@Override
		public JdbcContext getJdbcContext() {
			return null;
		}

		@Override
		public void prepare() {
		}

		@Override
		public Connection getIsolatedConnection() {
			return NoopConnection.INSTANCE;
		}

		@Override
		public void release() {
		}
	}

	private static class NoopConnection implements Connection {

		static final NoopConnection INSTANCE = new NoopConnection();

		private NoopConnection() {
		}

		@Override
		public Statement createStatement() throws SQLException {
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql) throws SQLException {
			return null;
		}

		@Override
		public CallableStatement prepareCall(String sql) throws SQLException {
			return null;
		}

		@Override
		public String nativeSQL(String sql) throws SQLException {
			return null;
		}

		@Override
		public void setAutoCommit(boolean autoCommit) throws SQLException {

		}

		@Override
		public boolean getAutoCommit() throws SQLException {
			return false;
		}

		@Override
		public void commit() throws SQLException {

		}

		@Override
		public void rollback() throws SQLException {

		}

		@Override
		public void close() throws SQLException {

		}

		@Override
		public boolean isClosed() throws SQLException {
			return false;
		}

		@Override
		public DatabaseMetaData getMetaData() throws SQLException {
			return null;
		}

		@Override
		public void setReadOnly(boolean readOnly) throws SQLException {

		}

		@Override
		public boolean isReadOnly() throws SQLException {
			return false;
		}

		@Override
		public void setCatalog(String catalog) throws SQLException {

		}

		@Override
		public String getCatalog() throws SQLException {
			return null;
		}

		@Override
		public void setTransactionIsolation(int level) throws SQLException {

		}

		@Override
		public int getTransactionIsolation() throws SQLException {
			return 0;
		}

		@Override
		public SQLWarning getWarnings() throws SQLException {
			return null;
		}

		@Override
		public void clearWarnings() throws SQLException {

		}

		@Override
		public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
				throws SQLException {
			return null;
		}

		@Override
		public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
				throws SQLException {
			return null;
		}

		@Override
		public Map<String, Class<?>> getTypeMap() throws SQLException {
			return null;
		}

		@Override
		public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

		}

		@Override
		public void setHoldability(int holdability) throws SQLException {

		}

		@Override
		public int getHoldability() throws SQLException {
			return 0;
		}

		@Override
		public Savepoint setSavepoint() throws SQLException {
			return null;
		}

		@Override
		public Savepoint setSavepoint(String name) throws SQLException {
			return null;
		}

		@Override
		public void rollback(Savepoint savepoint) throws SQLException {

		}

		@Override
		public void releaseSavepoint(Savepoint savepoint) throws SQLException {

		}

		@Override
		public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
				throws SQLException {
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(
				String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
			return null;
		}

		@Override
		public CallableStatement prepareCall(
				String sql,
				int resultSetType,
				int resultSetConcurrency,
				int resultSetHoldability) throws SQLException {
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
			return null;
		}

		@Override
		public Clob createClob() throws SQLException {
			return null;
		}

		@Override
		public Blob createBlob() throws SQLException {
			return null;
		}

		@Override
		public NClob createNClob() throws SQLException {
			return null;
		}

		@Override
		public SQLXML createSQLXML() throws SQLException {
			return null;
		}

		@Override
		public boolean isValid(int timeout) throws SQLException {
			return false;
		}

		@Override
		public void setClientInfo(String name, String value) throws SQLClientInfoException {

		}

		@Override
		public void setClientInfo(Properties properties) throws SQLClientInfoException {

		}

		@Override
		public String getClientInfo(String name) throws SQLException {
			return null;
		}

		@Override
		public Properties getClientInfo() throws SQLException {
			return null;
		}

		@Override
		public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
			return null;
		}

		@Override
		public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
			return null;
		}

		@Override
		public void setSchema(String schema) throws SQLException {

		}

		@Override
		public String getSchema() throws SQLException {
			return null;
		}

		@Override
		public void abort(Executor executor) throws SQLException {

		}

		@Override
		public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

		}

		@Override
		public int getNetworkTimeout() throws SQLException {
			return 0;
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			return null;
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return false;
		}
	}

	private static class ResultSetWorkaround implements ResultSet {
		private final ResultSet delegate;

		public ResultSetWorkaround(ResultSet delegate) {
			this.delegate = delegate;
		}

		@Override
		public boolean next() throws SQLException {
			return delegate.next();
		}

		@Override
		public void close() throws SQLException {
			delegate.close();
		}

		@Override
		public boolean wasNull() throws SQLException {
			return delegate.wasNull();
		}

		@Override
		public String getString(int columnIndex) throws SQLException {
			return delegate.getString(columnIndex);
		}

		@Override
		public boolean getBoolean(int columnIndex) throws SQLException {
			return delegate.getBoolean(columnIndex);
		}

		@Override
		public byte getByte(int columnIndex) throws SQLException {
			return delegate.getByte(columnIndex);
		}

		@Override
		public short getShort(int columnIndex) throws SQLException {
			return delegate.getShort(columnIndex);
		}

		@Override
		public int getInt(int columnIndex) throws SQLException {
			return delegate.getInt(columnIndex);
		}

		@Override
		public long getLong(int columnIndex) throws SQLException {
			return delegate.getLong(columnIndex);
		}

		@Override
		public float getFloat(int columnIndex) throws SQLException {
			return delegate.getFloat(columnIndex);
		}

		@Override
		public double getDouble(int columnIndex) throws SQLException {
			return delegate.getDouble(columnIndex);
		}

		@Override
		public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
			return delegate.getBigDecimal(columnIndex, scale);
		}

		@Override
		public byte[] getBytes(int columnIndex) throws SQLException {
			return delegate.getBytes(columnIndex);
		}

		@Override
		public Date getDate(int columnIndex) throws SQLException {
			return delegate.getDate(columnIndex);
		}

		@Override
		public Time getTime(int columnIndex) throws SQLException {
			return delegate.getTime(columnIndex);
		}

		@Override
		public Timestamp getTimestamp(int columnIndex) throws SQLException {
			return delegate.getTimestamp(columnIndex);
		}

		@Override
		public InputStream getAsciiStream(int columnIndex) throws SQLException {
			return delegate.getAsciiStream(columnIndex);
		}

		@Override
		public InputStream getUnicodeStream(int columnIndex) throws SQLException {
			return delegate.getUnicodeStream(columnIndex);
		}

		@Override
		public InputStream getBinaryStream(int columnIndex) throws SQLException {
			return delegate.getBinaryStream(columnIndex);
		}

		@Override
		public String getString(String columnLabel) throws SQLException {
			return delegate.getString(columnLabel);
		}

		@Override
		public boolean getBoolean(String columnLabel) throws SQLException {
			return delegate.getBoolean(columnLabel);
		}

		@Override
		public byte getByte(String columnLabel) throws SQLException {
			return delegate.getByte(columnLabel);
		}

		@Override
		public short getShort(String columnLabel) throws SQLException {
			return delegate.getShort(columnLabel);
		}

		@Override
		public int getInt(String columnLabel) throws SQLException {
			return delegate.getInt(columnLabel);
		}

		@Override
		public long getLong(String columnLabel) throws SQLException {
			// PostgreSQL stores sequence metadata in information_schema as Strings.
			// First try to get the value as a Long; if that fails, try to get
			// as a String and try to parse it as a Long.
			try {
				return delegate.getLong(columnLabel);
			}
			catch (ClassCastException ex) {
				// Check if the value is a String that can be converted to a Long
				final String aString;
				try {
					aString = delegate.getString(columnLabel);
				}
				catch (ClassCastException cce) {
					// The value is not a string either
					// Throw the original exception
					throw ex;
				}
				// aString won't be null; check just because...
				if (aString == null) {
					throw ex;
				}
				try {
					return Long.parseLong(aString);
				}
				catch (NumberFormatException nfe) {
					// The string cannot be parsed
					// Throw the original exception
					throw ex;
				}
			}
		}

		@Override
		public float getFloat(String columnLabel) throws SQLException {
			return delegate.getFloat(columnLabel);
		}

		@Override
		public double getDouble(String columnLabel) throws SQLException {
			return delegate.getDouble(columnLabel);
		}

		@Override
		public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
			return delegate.getBigDecimal(columnLabel, scale);
		}

		@Override
		public byte[] getBytes(String columnLabel) throws SQLException {
			return delegate.getBytes(columnLabel);
		}

		@Override
		public Date getDate(String columnLabel) throws SQLException {
			return delegate.getDate(columnLabel);
		}

		@Override
		public Time getTime(String columnLabel) throws SQLException {
			return delegate.getTime(columnLabel);
		}

		@Override
		public Timestamp getTimestamp(String columnLabel) throws SQLException {
			return delegate.getTimestamp(columnLabel);
		}

		@Override
		public InputStream getAsciiStream(String columnLabel) throws SQLException {
			return delegate.getAsciiStream(columnLabel);
		}

		@Override
		public InputStream getUnicodeStream(String columnLabel) throws SQLException {
			return delegate.getUnicodeStream(columnLabel);
		}

		@Override
		public InputStream getBinaryStream(String columnLabel) throws SQLException {
			return delegate.getBinaryStream(columnLabel);
		}

		@Override
		public SQLWarning getWarnings() throws SQLException {
			return delegate.getWarnings();
		}

		@Override
		public void clearWarnings() throws SQLException {
			delegate.clearWarnings();
		}

		@Override
		public String getCursorName() throws SQLException {
			return delegate.getCursorName();
		}

		@Override
		public ResultSetMetaData getMetaData() throws SQLException {
			return delegate.getMetaData();
		}

		@Override
		public Object getObject(int columnIndex) throws SQLException {
			return delegate.getObject(columnIndex);
		}

		@Override
		public Object getObject(String columnLabel) throws SQLException {
			return delegate.getObject(columnLabel);
		}

		@Override
		public int findColumn(String columnLabel) throws SQLException {
			return delegate.findColumn(columnLabel);
		}

		@Override
		public Reader getCharacterStream(int columnIndex) throws SQLException {
			return delegate.getCharacterStream(columnIndex);
		}

		@Override
		public Reader getCharacterStream(String columnLabel) throws SQLException {
			return delegate.getCharacterStream(columnLabel);
		}

		@Override
		public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
			return delegate.getBigDecimal(columnIndex);
		}

		@Override
		public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
			return delegate.getBigDecimal(columnLabel);
		}

		@Override
		public boolean isBeforeFirst() throws SQLException {
			return delegate.isBeforeFirst();
		}

		@Override
		public boolean isAfterLast() throws SQLException {
			return delegate.isAfterLast();
		}

		@Override
		public boolean isFirst() throws SQLException {
			return delegate.isFirst();
		}

		@Override
		public boolean isLast() throws SQLException {
			return delegate.isLast();
		}

		@Override
		public void beforeFirst() throws SQLException {
			delegate.beforeFirst();
		}

		@Override
		public void afterLast() throws SQLException {
			delegate.afterLast();
		}

		@Override
		public boolean first() throws SQLException {
			return delegate.first();
		}

		@Override
		public boolean last() throws SQLException {
			return delegate.last();
		}

		@Override
		public int getRow() throws SQLException {
			return delegate.getRow();
		}

		@Override
		public boolean absolute(int row) throws SQLException {
			return delegate.absolute(row);
		}

		@Override
		public boolean relative(int rows) throws SQLException {
			return delegate.relative(rows);
		}

		@Override
		public boolean previous() throws SQLException {
			return delegate.previous();
		}

		@Override
		public void setFetchDirection(int direction) throws SQLException {
			delegate.setFetchDirection(direction);
		}

		@Override
		public int getFetchDirection() throws SQLException {
			return delegate.getFetchDirection();
		}

		@Override
		public void setFetchSize(int rows) throws SQLException {
			delegate.setFetchSize(rows);
		}

		@Override
		public int getFetchSize() throws SQLException {
			return delegate.getFetchSize();
		}

		@Override
		public int getType() throws SQLException {
			return delegate.getType();
		}

		@Override
		public int getConcurrency() throws SQLException {
			return delegate.getConcurrency();
		}

		@Override
		public boolean rowUpdated() throws SQLException {
			return delegate.rowUpdated();
		}

		@Override
		public boolean rowInserted() throws SQLException {
			return delegate.rowInserted();
		}

		@Override
		public boolean rowDeleted() throws SQLException {
			return delegate.rowDeleted();
		}

		@Override
		public void updateNull(int columnIndex) throws SQLException {
			delegate.updateNull(columnIndex);
		}

		@Override
		public void updateBoolean(int columnIndex, boolean x) throws SQLException {
			delegate.updateBoolean(columnIndex, x);
		}

		@Override
		public void updateByte(int columnIndex, byte x) throws SQLException {
			delegate.updateByte(columnIndex, x);
		}

		@Override
		public void updateShort(int columnIndex, short x) throws SQLException {
			delegate.updateShort(columnIndex, x);
		}

		@Override
		public void updateInt(int columnIndex, int x) throws SQLException {
			delegate.updateInt(columnIndex, x);
		}

		@Override
		public void updateLong(int columnIndex, long x) throws SQLException {
			delegate.updateLong(columnIndex, x);
		}

		@Override
		public void updateFloat(int columnIndex, float x) throws SQLException {
			delegate.updateFloat(columnIndex, x);
		}

		@Override
		public void updateDouble(int columnIndex, double x) throws SQLException {
			delegate.updateDouble(columnIndex, x);
		}

		@Override
		public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
			delegate.updateBigDecimal(columnIndex, x);
		}

		@Override
		public void updateString(int columnIndex, String x) throws SQLException {
			delegate.updateString(columnIndex, x);
		}

		@Override
		public void updateBytes(int columnIndex, byte[] x) throws SQLException {
			delegate.updateBytes(columnIndex, x);
		}

		@Override
		public void updateDate(int columnIndex, Date x) throws SQLException {
			delegate.updateDate(columnIndex, x);
		}

		@Override
		public void updateTime(int columnIndex, Time x) throws SQLException {
			delegate.updateTime(columnIndex, x);
		}

		@Override
		public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
			delegate.updateTimestamp(columnIndex, x);
		}

		@Override
		public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
			delegate.updateAsciiStream(columnIndex, x, length);
		}

		@Override
		public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
			delegate.updateBinaryStream(columnIndex, x, length);
		}

		@Override
		public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
			delegate.updateCharacterStream(columnIndex, x, length);
		}

		@Override
		public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
			delegate.updateObject(columnIndex, x, scaleOrLength);
		}

		@Override
		public void updateObject(int columnIndex, Object x) throws SQLException {
			delegate.updateObject(columnIndex, x);
		}

		@Override
		public void updateNull(String columnLabel) throws SQLException {
			delegate.updateNull(columnLabel);
		}

		@Override
		public void updateBoolean(String columnLabel, boolean x) throws SQLException {
			delegate.updateBoolean(columnLabel, x);
		}

		@Override
		public void updateByte(String columnLabel, byte x) throws SQLException {
			delegate.updateByte(columnLabel, x);
		}

		@Override
		public void updateShort(String columnLabel, short x) throws SQLException {
			delegate.updateShort(columnLabel, x);
		}

		@Override
		public void updateInt(String columnLabel, int x) throws SQLException {
			delegate.updateInt(columnLabel, x);
		}

		@Override
		public void updateLong(String columnLabel, long x) throws SQLException {
			delegate.updateLong(columnLabel, x);
		}

		@Override
		public void updateFloat(String columnLabel, float x) throws SQLException {
			delegate.updateFloat(columnLabel, x);
		}

		@Override
		public void updateDouble(String columnLabel, double x) throws SQLException {
			delegate.updateDouble(columnLabel, x);
		}

		@Override
		public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
			delegate.updateBigDecimal(columnLabel, x);
		}

		@Override
		public void updateString(String columnLabel, String x) throws SQLException {
			delegate.updateString(columnLabel, x);
		}

		@Override
		public void updateBytes(String columnLabel, byte[] x) throws SQLException {
			delegate.updateBytes(columnLabel, x);
		}

		@Override
		public void updateDate(String columnLabel, Date x) throws SQLException {
			delegate.updateDate(columnLabel, x);
		}

		@Override
		public void updateTime(String columnLabel, Time x) throws SQLException {
			delegate.updateTime(columnLabel, x);
		}

		@Override
		public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
			delegate.updateTimestamp(columnLabel, x);
		}

		@Override
		public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
			delegate.updateAsciiStream(columnLabel, x, length);
		}

		@Override
		public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
			delegate.updateBinaryStream(columnLabel, x, length);
		}

		@Override
		public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
			delegate.updateCharacterStream(columnLabel, reader, length);
		}

		@Override
		public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
			delegate.updateObject(columnLabel, x, scaleOrLength);
		}

		@Override
		public void updateObject(String columnLabel, Object x) throws SQLException {
			delegate.updateObject(columnLabel, x);
		}

		@Override
		public void insertRow() throws SQLException {
			delegate.insertRow();
		}

		@Override
		public void updateRow() throws SQLException {
			delegate.updateRow();
		}

		@Override
		public void deleteRow() throws SQLException {
			delegate.deleteRow();
		}

		@Override
		public void refreshRow() throws SQLException {
			delegate.refreshRow();
		}

		@Override
		public void cancelRowUpdates() throws SQLException {
			delegate.cancelRowUpdates();
		}

		@Override
		public void moveToInsertRow() throws SQLException {
			delegate.moveToInsertRow();
		}

		@Override
		public void moveToCurrentRow() throws SQLException {
			delegate.moveToCurrentRow();
		}

		@Override
		public Statement getStatement() throws SQLException {
			return delegate.getStatement();
		}

		@Override
		public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
			return delegate.getObject(columnIndex, map);
		}

		@Override
		public Ref getRef(int columnIndex) throws SQLException {
			return delegate.getRef(columnIndex);
		}

		@Override
		public Blob getBlob(int columnIndex) throws SQLException {
			return delegate.getBlob(columnIndex);
		}

		@Override
		public Clob getClob(int columnIndex) throws SQLException {
			return delegate.getClob(columnIndex);
		}

		@Override
		public Array getArray(int columnIndex) throws SQLException {
			return delegate.getArray(columnIndex);
		}

		@Override
		public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
			return delegate.getObject(columnLabel, map);
		}

		@Override
		public Ref getRef(String columnLabel) throws SQLException {
			return delegate.getRef(columnLabel);
		}

		@Override
		public Blob getBlob(String columnLabel) throws SQLException {
			return delegate.getBlob(columnLabel);
		}

		@Override
		public Clob getClob(String columnLabel) throws SQLException {
			return delegate.getClob(columnLabel);
		}

		@Override
		public Array getArray(String columnLabel) throws SQLException {
			return delegate.getArray(columnLabel);
		}

		@Override
		public Date getDate(int columnIndex, Calendar cal) throws SQLException {
			return delegate.getDate(columnIndex, cal);
		}

		@Override
		public Date getDate(String columnLabel, Calendar cal) throws SQLException {
			return delegate.getDate(columnLabel, cal);
		}

		@Override
		public Time getTime(int columnIndex, Calendar cal) throws SQLException {
			return delegate.getTime(columnIndex, cal);
		}

		@Override
		public Time getTime(String columnLabel, Calendar cal) throws SQLException {
			return delegate.getTime(columnLabel, cal);
		}

		@Override
		public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
			return delegate.getTimestamp(columnIndex, cal);
		}

		@Override
		public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
			return delegate.getTimestamp(columnLabel, cal);
		}

		@Override
		public URL getURL(int columnIndex) throws SQLException {
			return delegate.getURL(columnIndex);
		}

		@Override
		public URL getURL(String columnLabel) throws SQLException {
			return delegate.getURL(columnLabel);
		}

		@Override
		public void updateRef(int columnIndex, Ref x) throws SQLException {
			delegate.updateRef(columnIndex, x);
		}

		@Override
		public void updateRef(String columnLabel, Ref x) throws SQLException {
			delegate.updateRef(columnLabel, x);
		}

		@Override
		public void updateBlob(int columnIndex, Blob x) throws SQLException {
			delegate.updateBlob(columnIndex, x);
		}

		@Override
		public void updateBlob(String columnLabel, Blob x) throws SQLException {
			delegate.updateBlob(columnLabel, x);
		}

		@Override
		public void updateClob(int columnIndex, Clob x) throws SQLException {
			delegate.updateClob(columnIndex, x);
		}

		@Override
		public void updateClob(String columnLabel, Clob x) throws SQLException {
			delegate.updateClob(columnLabel, x);
		}

		@Override
		public void updateArray(int columnIndex, Array x) throws SQLException {
			delegate.updateArray(columnIndex, x);
		}

		@Override
		public void updateArray(String columnLabel, Array x) throws SQLException {
			delegate.updateArray(columnLabel, x);
		}

		@Override
		public RowId getRowId(int columnIndex) throws SQLException {
			return delegate.getRowId(columnIndex);
		}

		@Override
		public RowId getRowId(String columnLabel) throws SQLException {
			return delegate.getRowId(columnLabel);
		}

		@Override
		public void updateRowId(int columnIndex, RowId x) throws SQLException {
			delegate.updateRowId(columnIndex, x);
		}

		@Override
		public void updateRowId(String columnLabel, RowId x) throws SQLException {
			delegate.updateRowId(columnLabel, x);
		}

		@Override
		public int getHoldability() throws SQLException {
			return delegate.getHoldability();
		}

		@Override
		public boolean isClosed() throws SQLException {
			return delegate.isClosed();
		}

		@Override
		public void updateNString(int columnIndex, String nString) throws SQLException {
			delegate.updateNString(columnIndex, nString);
		}

		@Override
		public void updateNString(String columnLabel, String nString) throws SQLException {
			delegate.updateNString(columnLabel, nString);
		}

		@Override
		public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
			delegate.updateNClob(columnIndex, nClob);
		}

		@Override
		public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
			delegate.updateNClob(columnLabel, nClob);
		}

		@Override
		public NClob getNClob(int columnIndex) throws SQLException {
			return delegate.getNClob(columnIndex);
		}

		@Override
		public NClob getNClob(String columnLabel) throws SQLException {
			return delegate.getNClob(columnLabel);
		}

		@Override
		public SQLXML getSQLXML(int columnIndex) throws SQLException {
			return delegate.getSQLXML(columnIndex);
		}

		@Override
		public SQLXML getSQLXML(String columnLabel) throws SQLException {
			return delegate.getSQLXML(columnLabel);
		}

		@Override
		public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
			delegate.updateSQLXML(columnIndex, xmlObject);
		}

		@Override
		public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
			delegate.updateSQLXML(columnLabel, xmlObject);
		}

		@Override
		public String getNString(int columnIndex) throws SQLException {
			return delegate.getNString(columnIndex);
		}

		@Override
		public String getNString(String columnLabel) throws SQLException {
			return delegate.getNString(columnLabel);
		}

		@Override
		public Reader getNCharacterStream(int columnIndex) throws SQLException {
			return delegate.getNCharacterStream(columnIndex);
		}

		@Override
		public Reader getNCharacterStream(String columnLabel) throws SQLException {
			return delegate.getNCharacterStream(columnLabel);
		}

		@Override
		public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
			delegate.updateNCharacterStream(columnIndex, x, length);
		}

		@Override
		public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
			delegate.updateNCharacterStream(columnLabel, reader, length);
		}

		@Override
		public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
			delegate.updateAsciiStream(columnIndex, x, length);
		}

		@Override
		public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
			delegate.updateBinaryStream(columnIndex, x, length);
		}

		@Override
		public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
			delegate.updateCharacterStream(columnIndex, x, length);
		}

		@Override
		public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
			delegate.updateAsciiStream(columnLabel, x, length);
		}

		@Override
		public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
			delegate.updateBinaryStream(columnLabel, x, length);
		}

		@Override
		public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
			delegate.updateCharacterStream(columnLabel, reader, length);
		}

		@Override
		public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
			delegate.updateBlob(columnIndex, inputStream, length);
		}

		@Override
		public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
			delegate.updateBlob(columnLabel, inputStream, length);
		}

		@Override
		public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
			delegate.updateClob(columnIndex, reader, length);
		}

		@Override
		public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
			delegate.updateClob(columnLabel, reader, length);
		}

		@Override
		public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
			delegate.updateNClob(columnIndex, reader, length);
		}

		@Override
		public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
			delegate.updateNClob(columnLabel, reader, length);
		}

		@Override
		public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
			delegate.updateNCharacterStream(columnIndex, x);
		}

		@Override
		public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
			delegate.updateNCharacterStream(columnLabel, reader);
		}

		@Override
		public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
			delegate.updateAsciiStream(columnIndex, x);
		}

		@Override
		public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
			delegate.updateBinaryStream(columnIndex, x);
		}

		@Override
		public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
			delegate.updateCharacterStream(columnIndex, x);
		}

		@Override
		public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
			delegate.updateAsciiStream(columnLabel, x);
		}

		@Override
		public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
			delegate.updateBinaryStream(columnLabel, x);
		}

		@Override
		public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
			delegate.updateCharacterStream(columnLabel, reader);
		}

		@Override
		public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
			delegate.updateBlob(columnIndex, inputStream);
		}

		@Override
		public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
			delegate.updateBlob(columnLabel, inputStream);
		}

		@Override
		public void updateClob(int columnIndex, Reader reader) throws SQLException {
			delegate.updateClob(columnIndex, reader);
		}

		@Override
		public void updateClob(String columnLabel, Reader reader) throws SQLException {
			delegate.updateClob(columnLabel, reader);
		}

		@Override
		public void updateNClob(int columnIndex, Reader reader) throws SQLException {
			delegate.updateNClob(columnIndex, reader);
		}

		@Override
		public void updateNClob(String columnLabel, Reader reader) throws SQLException {
			delegate.updateNClob(columnLabel, reader);
		}

		@Override
		public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
			return delegate.getObject(columnIndex, type);
		}

		@Override
		public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
			return delegate.getObject(columnLabel, type);
		}

		@Override
		public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
			delegate.updateObject(columnIndex, x, targetSqlType, scaleOrLength);
		}

		@Override
		public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
			delegate.updateObject(columnLabel, x, targetSqlType, scaleOrLength);
		}

		@Override
		public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
			delegate.updateObject(columnIndex, x, targetSqlType);
		}

		@Override
		public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
			delegate.updateObject(columnLabel, x, targetSqlType);
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return false;
		}
	}

}
