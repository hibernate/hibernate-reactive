/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.adaptor.impl;

import io.vertx.core.buffer.Buffer;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.engine.jdbc.BlobProxy;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Calendar;
import java.util.Map;

/**
 * An adaptor that allows Hibenate core code which expects a JDBC
 * {@code ResultSet} to read values from Vert.x's {@code RowSet}.
 */
public class ResultSetAdaptor implements ResultSet {

	private final RowIterator<Row> iterator;
	private final RowSet<Row> rows;
	private Row row;
	private boolean wasNull;

	public ResultSetAdaptor(RowSet<Row> rows) {
		this.iterator = rows.iterator();
		this.rows = rows;
	}

	@Override
	public boolean next() {
		if ( iterator.hasNext() ) {
			row = iterator.next();
			return true;
		}
		return false;
	}

	@Override
	public void close() {}

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
		wasNull = bool == null;
		return !wasNull && bool;
	}

	@Override
	public byte getByte(int columnIndex) {
		Integer integer = row.getInteger( columnIndex );
		wasNull = integer == null;
		return wasNull ? 0 : integer.byteValue();
	}

	@Override
	public short getShort(int columnIndex) {
		Short aShort = row.getShort( columnIndex );
		wasNull = aShort == null;
		return wasNull ? 0 : aShort;
	}

	@Override
	public int getInt(int columnIndex) {
		Integer integer = row.getInteger( columnIndex );
		wasNull = integer == null;
		return wasNull ? 0 : integer;
	}

	@Override
	public long getLong(int columnIndex) {
		Long aLong = row.getLong(columnIndex);
		wasNull = aLong == null;
		return wasNull ? 0 : aLong;
	}

	@Override
	public float getFloat(int columnIndex) {
		Float real = row.getFloat( columnIndex );
		wasNull = real == null;
		return wasNull ? 0 : real;
	}

	@Override
	public double getDouble(int columnIndex) {
		Double real = row.getDouble( columnIndex );
		wasNull = real == null;
		return wasNull ? 0 : real;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] getBytes(int columnIndex) {
		Buffer buffer = row.getBuffer(columnIndex);
		wasNull = buffer == null;
		return wasNull ? null : buffer.getBytes();
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
	public Time getTime(int columnIndex, Calendar cal) {
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
		Boolean bool = row.getBoolean( columnLabel );
		wasNull = bool == null;
		return !wasNull && bool;
	}

	@Override
	public byte getByte(String columnLabel) {
		Integer integer = row.getInteger( columnLabel );
		wasNull = integer == null;
		return wasNull ? 0 : integer.byteValue();
	}

	@Override
	public short getShort(String columnLabel) {
		Short aShort = row.getShort( columnLabel );
		wasNull = aShort == null;
		return wasNull ? 0 : aShort;
	}

	@Override
	public int getInt(String columnLabel) {
		Integer integer = row.getInteger( columnLabel );
		wasNull = integer == null;
		return wasNull ? 0 : integer;
	}

	@Override
	public long getLong(String columnLabel) {
		Long aLong = row.getLong( columnLabel );
		wasNull = aLong == null;
		return wasNull ? 0 : aLong;
	}

	@Override
	public float getFloat(String columnLabel) {
		Float real = row.getFloat( columnLabel );
		wasNull = real == null;
		return wasNull ? 0 : real;
	}

	@Override
	public double getDouble(String columnLabel) {
		Double real = row.getDouble( columnLabel );
		wasNull = real == null;
		return wasNull ? 0 : real;
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] getBytes(String columnLabel) {
		Buffer buffer = row.getBuffer( columnLabel );
		wasNull = buffer == null;
		return wasNull ? null : buffer.getBytes();
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
	public Time getTime(String columnLabel, Calendar cal) {
		LocalTime localTime = row.getLocalTime( columnLabel );
		return ( wasNull = localTime == null ) ? null : Time.valueOf( localTime );
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) {
		Object rawValue = row.getValue(columnLabel);
		return (wasNull=rawValue==null) ? null : Timestamp.valueOf( toLocalDateTime(rawValue) );
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) {
		Object rawValue = row.getValue(columnLabel);
		return (wasNull=rawValue==null) ? null : Timestamp.from( toOffsetDateTime(rawValue, cal).toInstant() );
	}

	private static LocalDateTime toLocalDateTime(Object rawValue) {
		if ( rawValue instanceof OffsetDateTime ) {
			return LocalDateTime.from( (OffsetDateTime) rawValue );
		}
		else if ( rawValue instanceof LocalDateTime ) {
			return (LocalDateTime) rawValue;
		}
		else {
			throw new IllegalArgumentException( "Unexpected type: " + rawValue.getClass() );
		}
	}

	private static OffsetDateTime toOffsetDateTime(Object rawValue, Calendar cal) {
		if ( rawValue instanceof OffsetDateTime ) {
			return (OffsetDateTime) rawValue;
		}
		else if ( rawValue instanceof LocalDateTime ) {
			return ( (LocalDateTime) rawValue ).atZone( cal.getTimeZone().toZoneId() ).toOffsetDateTime();
		}
		else {
			throw new IllegalArgumentException( "Unexpected type: " + rawValue.getClass() );
		}
	}

	@Override
	public int getHoldability() {
		return CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public boolean isClosed() {
		return false;
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) {
		T object = row.get(type, columnIndex);
		return (wasNull=object==null) ? null : object;
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) {
		T object = row.get( type, row.getColumnIndex(columnLabel) );
		return (wasNull=object==null) ? null : object;
	}

	@Override
	public <T> T unwrap(Class<T> iface) {
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) {
		return false;
	}

	@Override
	public SQLWarning getWarnings() {
		return null;
	}

	@Override
	public void clearWarnings() {}

	@Override
	public String getCursorName() {
		return null;
	}

	@Override
	public ResultSetMetaData getMetaData() {
		return new ResultSetMetaData() {
			@Override
			public int getColumnCount() {
				return rows.columnsNames().size();
			}

			@Override
			public int getColumnType(int column) {
				ColumnDescriptor descriptor = rows.columnDescriptors().get(column-1);
				return descriptor.isArray() ? Types.ARRAY : descriptor.jdbcType().getVendorTypeNumber();
			}

			@Override
			public String getColumnLabel(int column) {
				return rows.columnsNames().get(column-1);
			}

			@Override
			public String getColumnName(int column) {
				return rows.columnsNames().get(column-1);
			}

			@Override
			public boolean isAutoIncrement(int column) {
				return false;
			}

			@Override
			public boolean isCaseSensitive(int column) {
				return false;
			}

			@Override
			public boolean isSearchable(int column) {
				return false;
			}

			@Override
			public boolean isCurrency(int column) {
				return false;
			}

			@Override
			public int isNullable(int column) {
				return columnNullableUnknown;
			}

			@Override
			public boolean isSigned(int column) {
				return false;
			}

			@Override
			public int getColumnDisplaySize(int column) {
				return 0;
			}

			@Override
			public String getSchemaName(int column) {
				return null;
			}

			@Override
			public int getPrecision(int column) {
				return 0;
			}

			@Override
			public int getScale(int column) {
				return 0;
			}

			@Override
			public String getTableName(int column) {
				return null;
			}

			@Override
			public String getCatalogName(int column) {
				return null;
			}

			@Override
			public String getColumnTypeName(int column) {
				// This information is in rows.columnDescriptors().get( column-1 ).dataType.name
				// but does not appear to be accessible.
				return null;
			}

			@Override
			public boolean isReadOnly(int column) {
				return false;
			}

			@Override
			public boolean isWritable(int column) {
				return false;
			}

			@Override
			public boolean isDefinitelyWritable(int column) {
				return false;
			}

			@Override
			public String getColumnClassName(int column) {
				return null;
			}

			@Override
			public <T> T unwrap(Class<T> iface) {
				return null;
			}

			@Override
			public boolean isWrapperFor(Class<?> iface) {
				return false;
			}
		};
	}

	@Override
	public Object getObject(int columnIndex) {
		Object object = row.getValue( columnIndex );
		return (wasNull=object==null) ? null : object;
	}

	@Override
	public Object getObject(String columnLabel) {
		Object object = row.getValue( columnLabel );
		return (wasNull=object==null) ? null : object;
	}

	@Override
	public int findColumn(String columnLabel) {
		return rows.columnsNames().indexOf(columnLabel)+1;
//		return row.getColumnIndex( columnLabel );
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
	public void setFetchDirection(int direction) {}

	@Override
	public int getFetchDirection() {
		return FETCH_FORWARD;
	}

	@Override
	public void setFetchSize(int rows) {}

	@Override
	public int getFetchSize() {
		return 0;
	}

	@Override
	public int getType() {
		return TYPE_FORWARD_ONLY;
	}

	@Override
	public int getConcurrency() {
		return CONCUR_READ_ONLY;
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
	public boolean isBeforeFirst() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isAfterLast() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isFirst() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isLast() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void beforeFirst() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void afterLast() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean first() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean last() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean absolute(int row) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean relative(int rows) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean previous() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getRow() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void insertRow() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateRow() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void deleteRow() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void refreshRow() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void cancelRowUpdates() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void moveToInsertRow() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void moveToCurrentRow() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Statement getStatement() {
		throw new UnsupportedOperationException();
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Reader getCharacterStream(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Reader getCharacterStream(String columnLabel) {
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
		Buffer buffer = (Buffer) row.getValue( columnLabel );
		wasNull = buffer == null;
		return wasNull ? null : BlobProxy.generateProxy( buffer.getBytes() );
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
	public Timestamp getTimestamp(int columnIndex, Calendar cal) {
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
	public RowId getRowId(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public RowId getRowId(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNull(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateByte(int columnIndex, byte x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateShort(int columnIndex, short x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateInt(int columnIndex, int x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateLong(int columnIndex, long x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateFloat(int columnIndex, float x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateDouble(int columnIndex, double x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateString(int columnIndex, String x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateDate(int columnIndex, Date x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateTime(int columnIndex, Time x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateObject(int columnIndex, Object x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNull(String columnLabel) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateByte(String columnLabel, byte x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateShort(String columnLabel, short x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateInt(String columnLabel, int x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateLong(String columnLabel, long x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateFloat(String columnLabel, float x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateDouble(String columnLabel, double x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateString(String columnLabel, String x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateDate(String columnLabel, Date x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateTime(String columnLabel, Time x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateObject(String columnLabel, Object x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateRef(int columnIndex, Ref x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateRef(String columnLabel, Ref x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateClob(int columnIndex, Clob x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateClob(String columnLabel, Clob x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateArray(int columnIndex, Array x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateArray(String columnLabel, Array x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNString(int columnIndex, String nString) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNString(String columnLabel, String nString) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) {
		throw new UnsupportedOperationException();
	}

}
