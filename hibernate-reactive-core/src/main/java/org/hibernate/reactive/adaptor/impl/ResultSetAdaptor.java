package org.hibernate.reactive.adaptor.impl;

import io.vertx.core.buffer.Buffer;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import org.hibernate.cfg.NotYetImplementedException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Map;

/**
 * An adaptor that allows Hibenate core code which expects a JDBC
 * {@code ResultSet} to read values from Vert.x's {@code RowSet}.
 */
public class ResultSetAdaptor implements ResultSet {

	private final RowIterator<Row> iterator;
	private Row row;
	private boolean wasNull;

	public ResultSetAdaptor(RowSet<Row> rows) {
		this.iterator = rows.iterator();
	}

	@Override
	public boolean next() {
		if ( iterator.hasNext() ) {
			this.row = iterator.next();
			return true;
		}
		return false;
	}

	@Override
	public void close() {
	}

	@Override
	public boolean wasNull() {
		return wasNull;
	}

	@Override
	public String getString(int columnIndex) {
		String string = row.getString(columnIndex);
		return (wasNull=string==null) ? null : string;
	}

	@Override
	public boolean getBoolean(int columnIndex) {
		Boolean bool = row.getBoolean(columnIndex);
		return (wasNull=bool==null) ? false : bool;
	}

	@Override
	public byte getByte(int columnIndex) {
		Integer integer = row.getInteger( columnIndex );
		return (wasNull=integer==null) ? 0 : integer.byteValue();
	}

	@Override
	public short getShort(int columnIndex) {
		Short integer = row.getShort(columnIndex);
		return (wasNull=integer==null) ? 0 : integer;
	}

	@Override
	public int getInt(int columnIndex) {
		Integer integer = row.getInteger( columnIndex );
		return (wasNull=integer==null) ? 0 : integer;
	}

	@Override
	public long getLong(int columnIndex) {
		Long integer = row.getLong(columnIndex);
		return (wasNull=integer==null) ? 0 : integer;
	}

	@Override
	public float getFloat(int columnIndex) {
		Float real = row.getFloat(columnIndex);
		return (wasNull=real==null) ? 0 : real;
	}

	@Override
	public double getDouble(int columnIndex) {
		Double real = row.getDouble(columnIndex);
		return (wasNull=real==null) ? 0 : real;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] getBytes(int columnIndex) {
		Buffer buffer = row.getBuffer(columnIndex);
		return (wasNull=buffer==null) ? null : buffer.getBytes();
	}

	@Override
	public Date getDate(int columnIndex) {
		LocalDate localDate = row.getLocalDate(columnIndex);
		return (wasNull=localDate==null) ? null : java.sql.Date.valueOf(localDate);
	}

	@Override
	public Time getTime(int columnIndex) {
		LocalTime localTime = row.getLocalTime(columnIndex);
		return (wasNull=localTime==null) ? null : Time.valueOf(localTime);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) {
		LocalDateTime localDateTime = row.getLocalDateTime(columnIndex);
		return (wasNull=localDateTime==null) ? null : Timestamp.valueOf(localDateTime);
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) {
		throw new NotYetImplementedException( "This type hasn't been implemented yet" );
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) {
		throw new NotYetImplementedException( "This type hasn't been implemented yet" );
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) {
		throw new NotYetImplementedException( "This type hasn't been implemented yet" );
	}

	@Override
	public String getString(String columnLabel) {
		String string = row.getString(columnLabel);
		return (wasNull=string==null) ? null : string;
	}

	@Override
	public boolean getBoolean(String columnLabel) {
		Boolean bool = row.getBoolean(columnLabel);
		return (wasNull=bool==null) ? false : bool;
	}

	@Override
	public byte getByte(String columnLabel) {
		Integer integer = row.getInteger( columnLabel );
		return (wasNull=integer==null) ? 0 : integer.byteValue();
	}

	@Override
	public short getShort(String columnLabel) {
		Short integer = row.getShort(columnLabel);
		return (wasNull=integer==null) ? 0 : integer;
	}

	@Override
	public int getInt(String columnLabel) {
		Integer integer = row.getInteger( columnLabel );
		return (wasNull=integer==null) ? 0 : integer;
	}

	@Override
	public long getLong(String columnLabel) {
		Long integer = row.getLong(columnLabel);
		return (wasNull=integer==null) ? 0 : integer;
	}

	@Override
	public float getFloat(String columnLabel) {
		Float real = row.getFloat(columnLabel);
		return (wasNull=real==null) ? 0 : real;
	}

	@Override
	public double getDouble(String columnLabel) {
		Double real = row.getDouble(columnLabel);
		return (wasNull=real==null) ? 0 : real;
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] getBytes(String columnLabel) {
		Buffer buffer = row.getBuffer(columnLabel);
		return (wasNull=buffer==null) ? null : buffer.getBytes();
	}

	@Override
	public Date getDate(String columnLabel) {
		LocalDate localDate = row.getLocalDate(columnLabel);
		return (wasNull=localDate==null) ? null : java.sql.Date.valueOf(localDate);
	}

	@Override
	public Time getTime(String columnLabel) {
		LocalTime localTime = row.getLocalTime(columnLabel);
		return (wasNull=localTime==null) ? null : Time.valueOf(localTime);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) {
		LocalDateTime localDateTime = row.getLocalDateTime(columnLabel);
		return (wasNull=localDateTime==null) ? null : Timestamp.valueOf(localDateTime);
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) {
		throw new NotYetImplementedException( "This type hasn't been implemented yet" );
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) {
		throw new NotYetImplementedException( "This type hasn't been implemented yet" );
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) {
		throw new NotYetImplementedException( "This type hasn't been implemented yet" );
	}

	@Override
	public SQLWarning getWarnings() {
		return null;
	}

	@Override
	public void clearWarnings() {

	}

	@Override
	public String getCursorName() {
		return null;
	}

	@Override
	public ResultSetMetaData getMetaData() {
		return null;
	}

	@Override
	public Object getObject(int columnIndex) {
		return row.getValue( columnIndex );
	}

	@Override
	public Object getObject(String columnLabel) {
		return row.getValue( columnLabel );
	}

	@Override
	public int findColumn(String columnLabel) {
		return row.getColumnIndex( columnLabel );
	}

	@Override
	public Reader getCharacterStream(int columnIndex) {
		throw new NotYetImplementedException( "This type hasn't been implemented yet" );
	}

	@Override
	public Reader getCharacterStream(String columnLabel) {
		throw new NotYetImplementedException( "This type hasn't been implemented yet" );
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) {
		BigDecimal decimal = row.getBigDecimal(columnIndex);
		return (wasNull=decimal==null) ? null : decimal;
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) {
		BigDecimal decimal = row.getBigDecimal(columnLabel);
		return (wasNull=decimal==null) ? null : decimal;
	}

	@Override
	public boolean isBeforeFirst() {
		return false;
	}

	@Override
	public boolean isAfterLast() {
		return false;
	}

	@Override
	public boolean isFirst() {
		return false;
	}

	@Override
	public boolean isLast() {
		return false;
	}

	@Override
	public void beforeFirst() {

	}

	@Override
	public void afterLast() {

	}

	@Override
	public boolean first() {
		return false;
	}

	@Override
	public boolean last() {
		return false;
	}

	@Override
	public int getRow() {
		return 0;
	}

	@Override
	public boolean absolute(int row) {
		return false;
	}

	@Override
	public boolean relative(int rows) {
		return false;
	}

	@Override
	public boolean previous() {
		return false;
	}

	@Override
	public void setFetchDirection(int direction) {

	}

	@Override
	public int getFetchDirection() {
		return 0;
	}

	@Override
	public void setFetchSize(int rows) {

	}

	@Override
	public int getFetchSize() {
		return 0;
	}

	@Override
	public int getType() {
		return 0;
	}

	@Override
	public int getConcurrency() {
		return 0;
	}

	@Override
	public boolean rowUpdated() {
		return false;
	}

	@Override
	public boolean rowInserted() {
		return false;
	}

	@Override
	public boolean rowDeleted() {
		return false;
	}

	@Override
	public void updateNull(int columnIndex) {

	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) {

	}

	@Override
	public void updateByte(int columnIndex, byte x) {

	}

	@Override
	public void updateShort(int columnIndex, short x) {

	}

	@Override
	public void updateInt(int columnIndex, int x) {

	}

	@Override
	public void updateLong(int columnIndex, long x) {

	}

	@Override
	public void updateFloat(int columnIndex, float x) {

	}

	@Override
	public void updateDouble(int columnIndex, double x) {

	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) {

	}

	@Override
	public void updateString(int columnIndex, String x) {

	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) {

	}

	@Override
	public void updateDate(int columnIndex, Date x) {

	}

	@Override
	public void updateTime(int columnIndex, Time x) {

	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) {

	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) {

	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) {

	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) {

	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) {

	}

	@Override
	public void updateObject(int columnIndex, Object x) {

	}

	@Override
	public void updateNull(String columnLabel) {

	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) {

	}

	@Override
	public void updateByte(String columnLabel, byte x) {

	}

	@Override
	public void updateShort(String columnLabel, short x) {

	}

	@Override
	public void updateInt(String columnLabel, int x) {

	}

	@Override
	public void updateLong(String columnLabel, long x) {

	}

	@Override
	public void updateFloat(String columnLabel, float x) {

	}

	@Override
	public void updateDouble(String columnLabel, double x) {

	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) {

	}

	@Override
	public void updateString(String columnLabel, String x) {

	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) {

	}

	@Override
	public void updateDate(String columnLabel, Date x) {

	}

	@Override
	public void updateTime(String columnLabel, Time x) {

	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) {

	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) {

	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) {

	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) {

	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) {

	}

	@Override
	public void updateObject(String columnLabel, Object x) {

	}

	@Override
	public void insertRow() {

	}

	@Override
	public void updateRow() {

	}

	@Override
	public void deleteRow() {

	}

	@Override
	public void refreshRow() {

	}

	@Override
	public void cancelRowUpdates() {

	}

	@Override
	public void moveToInsertRow() {

	}

	@Override
	public void moveToCurrentRow() {

	}

	@Override
	public Statement getStatement() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Ref getRef(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Blob getBlob(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Clob getClob(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Array getArray(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Ref getRef(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Blob getBlob(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Clob getClob(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Array getArray(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) {
		throw new UnsupportedOperationException();
	}

	@Override
	public URL getURL(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public URL getURL(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateRef(int columnIndex, Ref x) {

	}

	@Override
	public void updateRef(String columnLabel, Ref x) {

	}

	@Override
	public void updateBlob(int columnIndex, Blob x) {

	}

	@Override
	public void updateBlob(String columnLabel, Blob x) {

	}

	@Override
	public void updateClob(int columnIndex, Clob x) {

	}

	@Override
	public void updateClob(String columnLabel, Clob x) {

	}

	@Override
	public void updateArray(int columnIndex, Array x) {

	}

	@Override
	public void updateArray(String columnLabel, Array x) {

	}

	@Override
	public RowId getRowId(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public RowId getRowId(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) {

	}

	@Override
	public void updateRowId(String columnLabel, RowId x) {

	}

	@Override
	public int getHoldability() {
		return 0;
	}

	@Override
	public boolean isClosed() {
		return false;
	}

	@Override
	public void updateNString(int columnIndex, String nString) {

	}

	@Override
	public void updateNString(String columnLabel, String nString) {

	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) {

	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) {

	}

	@Override
	public NClob getNClob(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public NClob getNClob(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) {

	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) {

	}

	@Override
	public String getNString(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getNString(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) {

	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) {

	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) {

	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) {

	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) {

	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) {

	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) {

	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) {

	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) {

	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) {

	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) {

	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) {

	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) {

	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) {

	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) {

	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) {

	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) {

	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) {

	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) {

	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) {

	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) {

	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) {

	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) {

	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) {

	}

	@Override
	public void updateClob(int columnIndex, Reader reader) {

	}

	@Override
	public void updateClob(String columnLabel, Reader reader) {

	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) {

	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) {

	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) {
		return row.get(type, columnIndex);
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) {
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) {
		return false;
	}
}
