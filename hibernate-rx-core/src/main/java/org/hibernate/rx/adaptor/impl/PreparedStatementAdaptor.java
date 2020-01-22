package org.hibernate.rx.adaptor.impl;

import io.vertx.core.buffer.Buffer;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Arrays;
import java.util.Calendar;

/**
 * Collects parameter bindings from Hibernate core code
 * that expects a JDBC {@link PreparedStatement}.
 */
public class PreparedStatementAdaptor implements PreparedStatement {

	static final Object[] NO_PARAMS = new Object[0];

	Object[] params = NO_PARAMS;
	int size = 0;

	void put(int parameterIndex, Object parameter) {
		if ( params.length <= parameterIndex ) {
			params = Arrays.copyOf(params, 4 + parameterIndex * 2);
		}
		params[parameterIndex-1] = parameter;
		if ( size < parameterIndex ) {
			size = parameterIndex;
		}
	}

	void clear() {
		params = NO_PARAMS;
	}

	public Object[] getParametersAsArray() {
		return Arrays.copyOf(params, size);
	}

	@Override
	public ResultSet executeQuery() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int executeUpdate() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) {
		put( parameterIndex, null );
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) {
		put( parameterIndex, x );
	}

	@Override
	public void setByte(int parameterIndex, byte x) {
		put( parameterIndex, x );
	}

	@Override
	public void setShort(int parameterIndex, short x) {
		put( parameterIndex, x );
	}

	@Override
	public void setInt(int parameterIndex, int x) {
		put( parameterIndex, x );
	}

	@Override
	public void setLong(int parameterIndex, long x) {
		put( parameterIndex, x );
	}

	@Override
	public void setFloat(int parameterIndex, float x) {
		put( parameterIndex, x );
	}

	@Override
	public void setDouble(int parameterIndex, double x) {
		put( parameterIndex, x );
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) {
		put( parameterIndex, x );
	}

	@Override
	public void setString(int parameterIndex, String x) {
		put( parameterIndex, x );
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) {
		put( parameterIndex, Buffer.buffer(x) );
	}

	@Override
	public void setDate(int parameterIndex, Date x) {
		put( parameterIndex, x.toLocalDate() );
	}

	@Override
	public void setTime(int parameterIndex, Time x) {
		put( parameterIndex, x.toLocalTime() );
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) {
		put( parameterIndex, x.toLocalDateTime() );
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clearParameters() {
		clear();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) {
		put( parameterIndex, x );
//		throw new UnsupportedOperationException();
	}

	@Override
	public void setObject(int parameterIndex, Object x) {
		put( parameterIndex, x );
	}

	@Override
	public boolean execute() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addBatch() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setRef(int parameterIndex, Ref x) {
		put( parameterIndex, x );
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) {
		put( parameterIndex, x );
	}

	@Override
	public void setClob(int parameterIndex, Clob x) {
		put( parameterIndex, x );
	}

	@Override
	public void setArray(int parameterIndex, Array x) {
		put( parameterIndex, x );
	}

	@Override
	public ResultSetMetaData getMetaData() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setURL(int parameterIndex, URL x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ParameterMetaData getParameterMetaData() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNString(int parameterIndex, String value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) {
		put( parameterIndex,value );
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ResultSet executeQuery(String sql) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int executeUpdate(String sql) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {

	}

	@Override
	public int getMaxFieldSize() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setMaxFieldSize(int max) {

	}

	@Override
	public int getMaxRows() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setMaxRows(int max) {

	}

	@Override
	public void setEscapeProcessing(boolean enable) {

	}

	@Override
	public int getQueryTimeout() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setQueryTimeout(int seconds) {

	}

	@Override
	public void cancel() {

	}

	@Override
	public SQLWarning getWarnings() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clearWarnings() {

	}

	@Override
	public void setCursorName(String name) {

	}

	@Override
	public boolean execute(String sql) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ResultSet getResultSet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getUpdateCount() {
		return 0;
	}

	@Override
	public boolean getMoreResults() {
		return false;
	}

	@Override
	public int getFetchDirection() {
		return 0;
	}

	@Override
	public void setFetchDirection(int direction) {

	}

	@Override
	public int getFetchSize() {
		return 0;
	}

	@Override
	public void setFetchSize(int rows) {

	}

	@Override
	public int getResultSetConcurrency() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getResultSetType() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addBatch(String sql) {

	}

	@Override
	public void clearBatch() {

	}

	@Override
	public int[] executeBatch() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Connection getConnection() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean getMoreResults(int current) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ResultSet getGeneratedKeys() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean execute(String sql, String[] columnNames) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getResultSetHoldability() {
		return 0;
	}

	@Override
	public boolean isClosed() {
		return false;
	}

	@Override
	public boolean isPoolable() {
		return false;
	}

	@Override
	public void setPoolable(boolean poolable) {
	}

	@Override
	public void closeOnCompletion() {
	}

	@Override
	public boolean isCloseOnCompletion() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) {
		throw new UnsupportedOperationException();
	}
}
