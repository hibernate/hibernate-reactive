/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.adaptor.impl;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.hibernate.engine.jdbc.proxy.BlobProxy;
import org.hibernate.engine.jdbc.proxy.ClobProxy;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.type.descriptor.jdbc.JdbcType;

import io.vertx.core.buffer.Buffer;
import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import io.vertx.sqlclient.impl.RowBase;

import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

/**
 * An adaptor that allows Hibernate core code which expects a JDBC
 * {@code ResultSet} to read values from Vert.x's {@code RowSet}.
 */
public class ResultSetAdaptor implements ResultSet {

	private static final Log LOG = make( Log.class, lookup() );

	private final Iterator<? extends Row> iterator;

	private final List<ColumnDescriptor> columnDescriptors;
	private final List<String> columnNames;
	private Row row;
	private boolean wasNull;

	public ResultSetAdaptor(Object id, Class<?> idClass, String columnName) {
		requireNonNull( id );
		this.iterator = List.of( new RowAdaptor( id, idClass, columnName ) ).iterator();
		this.columnNames = columnName == null ? emptyList() : List.of( columnName );
		this.columnDescriptors = List.of( toColumnDescriptor( idClass, columnName ) );
	}

	private static class RowAdaptor implements Row {
		private final Object id;
		private final Class<?> idClass;
		private final String columnName;

		public RowAdaptor(Object id, Class<?> idClass, String columnName) {
			this.id = id;
			this.idClass = idClass;
			this.columnName = columnName;
		}

		@Override
		public Object getValue(String column) {
			return id;
		}

		@Override
		public String getColumnName(int pos) {
			return columnName;
		}

		@Override
		public int getColumnIndex(String column) {
			return 0;
		}

		@Override
		public Object getValue(int pos) {
			return id;
		}

		@Override
		public Tuple addValue(Object value) {
			return null;
		}

		@Override
		public int size() {
			return 1;
		}

		@Override
		public void clear() {
		}

		@Override
		public List<Class<?>> types() {
			return List.of( idClass );
		}
	}

	public ResultSetAdaptor(RowSet<Row> rows) {
		requireNonNull( rows );
		this.iterator = rows.iterator();
		this.columnNames = rows.columnsNames() == null ? emptyList() : rows.columnsNames();
		this.columnDescriptors = rows.columnDescriptors();
	}

	public ResultSetAdaptor(RowSet<Row> rows, PropertyKind<Row> propertyKind, List<String> generatedColumnNames, List<Class<?>> generatedColumnClasses) {
		this( rows, rows.property( propertyKind ), generatedColumnNames, generatedColumnClasses );
	}

	public ResultSetAdaptor(RowSet<Row> rows, Collection<?> ids, String idColumnName, Class<?> idClass) {
		this( rows, new RowFromId( ids, idColumnName ), idColumnName, idClass );
	}

	private ResultSetAdaptor(RowSet<Row> rows, Row row, String idColumnName, Class<?> idClass) {
		this( rows, row, List.of( idColumnName ), List.of( idClass ) );
	}

	private ResultSetAdaptor(RowSet<Row> rows, Row row, List<String> columnNames, List<Class<?>> columnClasses) {
		requireNonNull( rows );
		requireNonNull( columnNames );
		this.iterator = List.of( row ).iterator();
		this.columnNames =  columnNames ;
		this.columnDescriptors = new ArrayList<>(columnNames.size());
		for (int i =0; i < columnNames.size(); i++) {
			columnDescriptors.add( toColumnDescriptor( columnClasses.get( i ), columnNames.get(i) ) );
		}
	}

	private static ColumnDescriptor toColumnDescriptor(Class<?> idClass, String idColumnName) {
		return new ColumnDescriptor() {
			@Override
			public String name() {
				return idColumnName;
			}

			@Override
			public boolean isArray() {
				return idClass.isArray();
			}

			@Override
			public String typeName() {
				return idClass.getName();
			}

			@Override
			public JDBCType jdbcType() {
				return null;
			}
		};
	}

	private static class RowFromId extends RowBase {

		private final List<String> columns;

		public RowFromId(Collection<?> ids, String columnName) {
			super( ids );
			this.columns = List.of( requireNonNull( columnName ) );
		}

		@Override
		public String getColumnName(int pos) {
			return pos > 0 ? null : columns.get( pos );
		}

		@Override
		public int getColumnIndex(String column) {
			return columns.indexOf( column );
		}
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
	public void close() {
	}

	@Override
	public boolean wasNull() {
		return wasNull;
	}

	@Override
	public String getString(int columnIndex) {
		String string = row.getString( columnIndex - 1 );
		return ( wasNull = string == null ) ? null : string;
	}

	@Override
	public boolean getBoolean(int columnIndex) {
		try {
			Boolean bool = row.getBoolean( columnIndex - 1 );
			wasNull = bool == null;
			return !wasNull && bool;
		}
		catch (ClassCastException cce) {
			// Oracle doesn't support an actual boolean/Boolean datatype.
			// Oracle8iDialect in ORM registers the BOOLEAN type as a 'number( 1, 0 )'
			// so we need to convert the int to a boolean
			try {
				return getInt( columnIndex ) != 0;
			}
			catch (Exception e) {
				// ignore second exception and throw first cce
				throw cce;
			}
		}
	}

	@Override
	public byte getByte(int columnIndex) {
		Integer integer = row.getInteger( columnIndex - 1 );
		wasNull = integer == null;
		return wasNull ? 0 : integer.byteValue();
	}

	@Override
	public short getShort(int columnIndex) {
		Short aShort = row.getShort( columnIndex - 1 );
		wasNull = aShort == null;
		return wasNull ? 0 : aShort;
	}

	@Override
	public int getInt(int columnIndex) {
		Integer integer = row.getInteger( columnIndex - 1 );
		wasNull = integer == null;
		return wasNull ? 0 : integer;
	}

	@Override
	public long getLong(int columnIndex) {
		Long aLong = row.getLong( columnIndex - 1 );
		wasNull = aLong == null;
		return wasNull ? 0 : aLong;
	}

	@Override
	public float getFloat(int columnIndex) {
		Float real = row.getFloat( columnIndex - 1 );
		wasNull = real == null;
		return wasNull ? 0 : real;
	}

	@Override
	public double getDouble(int columnIndex) {
		Double real = row.getDouble( columnIndex - 1 );
		wasNull = real == null;
		return wasNull ? 0 : real;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] getBytes(int columnIndex) {
		Buffer buffer = row.getBuffer( columnIndex - 1 );
		wasNull = buffer == null;
		return wasNull ? null : buffer.getBytes();
	}

	@Override
	public Date getDate(int columnIndex) {
		LocalDate localDate = row.getLocalDate( columnIndex - 1 );
		return ( wasNull = localDate == null ) ? null : Date.valueOf( localDate );
	}

	@Override
	public Time getTime(int columnIndex) {
		LocalTime localTime = row.getLocalTime( columnIndex - 1 );
		return ( wasNull = localTime == null ) ? null : Time.valueOf( localTime );
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) {
		LocalTime localTime = row.getLocalTime( columnIndex - 1 );
		return ( wasNull = localTime == null ) ? null : Time.valueOf( localTime );
	}


	@Override
	public InputStream getAsciiStream(int columnIndex) {
		throw new UnsupportedOperationException( "This type hasn't been implemented yet" );
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) {
		throw new UnsupportedOperationException( "This type hasn't been implemented yet" );
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) {
		throw new UnsupportedOperationException( "This type hasn't been implemented yet" );
	}

	@Override
	public String getString(String columnLabel) {
		String result = caseInsensitiveGet( columnLabel, row::getString );
		wasNull = result == null;
		return result;
	}

	private <T> T caseInsensitiveGet(String columnLabel, Function<String, T> produce) {
		for ( String columnName : getColumnsNames() ) {
			if ( columnName.equalsIgnoreCase( columnLabel ) ) {
				return produce.apply( columnName );
			}
		}

		// Same error thrown by io.vertx.sqlclient.Row when it doesn't find the label
		throw new NoSuchElementException( "Column " + columnLabel + " does not exist" );
	}

	/**
	 * rows.columnsNames() might return null for some databases.
	 * For example, when creating a stored procedure in PostgreSQL using a native query.
	 *
	 * @return A list of column names or an empty list.
	 */
	private List<String> getColumnsNames() {
		return this.columnNames;
	}

	@Override
	public boolean getBoolean(String columnLabel) {
		try {
			Boolean bool = row.getBoolean( columnLabel );
			wasNull = bool == null;
			return !wasNull && bool;
		}
		catch (ClassCastException cce) {
			// Oracle doesn't support an actual boolean/Boolean datatype.
			// Oracle8iDialect in ORM registers the BOOLEAN type as a 'number( 1, 0 )'
			// so we need to convert the int to a boolean
			try {
				return getInt( columnLabel ) != 0;
			}
			catch (Exception e) {
				// ignore second exception and throw first cce
				throw cce;
			}
		}
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
		Long aLong = caseInsensitiveGet( columnLabel, row::getLong );
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
		LocalDate localDate = row.getLocalDate( columnLabel );
		return ( wasNull = localDate == null ) ? null : Date.valueOf( localDate );
	}

	@Override
	public Time getTime(String columnLabel) {
		LocalTime localTime = row.getLocalTime( columnLabel );
		return ( wasNull = localTime == null ) ? null : Time.valueOf( localTime );
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) {
		LocalTime localTime = row.getLocalTime( columnLabel );
		return ( wasNull = localTime == null ) ? null : Time.valueOf( localTime );
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) {
		LocalDateTime rawValue = row.getLocalDateTime( columnLabel );
		return ( wasNull = rawValue == null ) ? null : Timestamp.valueOf( rawValue );
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) {
		LocalDateTime localDateTime = row.getLocalDateTime( columnLabel );
		return ( wasNull = localDateTime == null ) ? null : toTimestamp( localDateTime, cal );
	}

	private static Timestamp toTimestamp(LocalDateTime localDateTime, Calendar cal) {
		return Timestamp.from( localDateTime.atZone( cal.getTimeZone().toZoneId() ).toInstant() );
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
		T object = row.get( type, columnIndex - 1 );
		return ( wasNull = object == null ) ? null : object;
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) {
		T object = row.get( type, row.getColumnIndex( columnLabel ) );
		return ( wasNull = object == null ) ? null : object;
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
	public void clearWarnings() {
	}

	@Override
	public String getCursorName() {
		return null;
	}

	@Override
	public Object getObject(int columnIndex) {
		Object object = row.getValue( columnIndex - 1 );
		return ( wasNull = object == null ) ? null : object;
	}

	@Override
	public Object getObject(String columnLabel) {
		Object object = row.getValue( columnLabel );
		return ( wasNull = object == null ) ? null : object;
	}

	@Override
	public int findColumn(String columnLabel) {
		// JDBC parameters index start from 1
		int index = 1;
		for ( String column : getColumnsNames() ) {
			// Some dbs, like Oracle and Db2, return the column names always in uppercase
			if ( column.equalsIgnoreCase( columnLabel ) ) {
				return index;
			}
			index++;
		}
		return -1;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) {
		BigDecimal decimal = row.getBigDecimal( columnIndex - 1 );
		return ( wasNull = decimal == null ) ? null : decimal;
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) {
		BigDecimal decimal = row.getBigDecimal( columnLabel );
		return ( wasNull = decimal == null ) ? null : decimal;
	}

	@Override
	public void setFetchDirection(int direction) {
	}

	@Override
	public int getFetchDirection() {
		return FETCH_FORWARD;
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
		return new PreparedStatementAdaptor();
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
		Blob blob = blob( row -> row.getValue( columnIndex - 1 ), row -> row.getBuffer( columnIndex - 1 ) );
		wasNull = blob == null;
		return blob;
	}

	@Override
	public Clob getClob(int columnIndex) {
		Clob clob = clob( row -> row.getString( columnIndex - 1 ) );
		wasNull = clob == null;
		if ( wasNull ) {
			return null;
		}
		return clob;
	}

	private Clob clob(Function<Row, String> getString) {
		String value = getString.apply( row );
		if ( value == null ) {
			return null;
		}

		return ClobProxy.generateProxy( ( value ) );
	}

	@Override
	public Array getArray(int columnIndex) {
		throw new UnsupportedOperationException();
	}

	public Array getArray(int columnIndex, JdbcType elementJdbcType) {
		Object[] objects = (Object[]) row.getValue( columnIndex - 1 );
		wasNull = objects == null;
		if ( objects == null ) {
			return null;
		}
		return new ArrayAdaptor( elementJdbcType, objects );
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
		Blob blob = blob( row -> row.getValue( columnLabel ), row -> row.getBuffer( columnLabel ) );
		wasNull = blob == null;
		return blob;
	}

	private Blob blob(Function<Row, Object> getValue, Function<Row, Buffer> getBuffer) {
		Object value = getValue.apply( row );
		if ( value == null ) {
			return null;
		}
		if ( value instanceof String ) {
			return BlobProxy.generateProxy( ( (String) value ).getBytes() );
		}
		if ( value instanceof byte[] ) {
			return BlobProxy.generateProxy( (byte[]) value );
		}
		return BlobProxy.generateProxy( getBuffer.apply( row ).getBytes() );
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
	public Timestamp getTimestamp(int columnIndex) {
		LocalDateTime localDateTime = row.getLocalDateTime( columnIndex - 1 );
		return ( wasNull = localDateTime == null ) ? null : Timestamp.valueOf( localDateTime );
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) {
		LocalDateTime localDateTime = row.getLocalDateTime( columnIndex - 1 );
		return ( wasNull = localDateTime == null ) ? null : toTimestamp( localDateTime, cal );
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
		return getString( columnIndex );
	}

	@Override
	public String getNString(String columnLabel) {
		return getString( columnLabel );
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
		throw LOG.unsupportedXmlType();
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) {
		throw LOG.unsupportedXmlType();
	}

	@Override
	public RowId getRowId(int columnIndex) {
		Buffer buffer = row.getBuffer( columnIndex - 1 );
		wasNull = buffer == null;
		return wasNull ? null : new RowIdAdaptor( buffer );
	}

	@Override
	public RowId getRowId(String columnLabel) {
		Buffer buffer = row.getBuffer( columnLabel );
		wasNull = buffer == null;
		return wasNull ? null : new RowIdAdaptor( buffer );
	}

	private static class RowIdAdaptor implements RowId {

		private final Buffer buffer;

		private RowIdAdaptor(Buffer buffer) {
			requireNonNull( buffer );
			this.buffer = buffer;
		}

		@Override
		public byte[] getBytes() {
			return buffer.getBytes();
		}
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


	@Override
	public ResultSetMetaData getMetaData() {
		return new MetaData( columnNames, columnDescriptors );
	}

	private static class MetaData implements ResultSetMetaData {

		private final List<String> columns;
		private final List<ColumnDescriptor> descriptors;
		private final String[] typeNames;


		public MetaData(List<String> columnNames, List<ColumnDescriptor> columnDescriptors) {
			columns = columnNames;
			descriptors = columnDescriptors;
			typeNames = initTypeNames( columnDescriptors );
		}

		private static String[] initTypeNames(List<ColumnDescriptor> columnDescriptors) {
			if ( columnDescriptors == null ) {
				return null;
			}
			final String[] typeNames = new String[columnDescriptors.size()];
			int i = 0;
			for ( ColumnDescriptor columnDescriptor : columnDescriptors ) {
				typeNames[i++] = columnDescriptor.typeName();
			}
			return typeNames;
		}

		@Override
		public int getColumnCount() {
			return columns.size();
		}

		@Override
		public int getColumnType(int column) {
			ColumnDescriptor descriptor = descriptors.get( column - 1 );
			return descriptor.isArray() ? Types.ARRAY : descriptor.jdbcType().getVendorTypeNumber();
		}

		@Override
		public String getColumnLabel(int column) {
			return columns.get( column - 1 );
		}

		@Override
		public String getColumnName(int column) {
			return columns.get( column - 1 );
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
			return typeNames[column - 1];
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
	}
}
