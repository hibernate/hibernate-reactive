/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.adaptor.impl;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.hibernate.type.descriptor.jdbc.JdbcType;

public class ArrayAdapter implements Array {
	private JdbcType elementJdbcType;
	private Object[] objects;
	public ArrayAdapter(JdbcType elementJdbcType, Object[] objects) {
		this.elementJdbcType = elementJdbcType;
		this.objects = objects;
	}

	@Override
	public String getBaseTypeName() throws SQLException {
		return elementJdbcType.getFriendlyName();
	}

	@Override
	public int getBaseType() throws SQLException {
		return elementJdbcType.getJdbcTypeCode();
	}

	@Override
	public Object getArray() throws SQLException {
		return objects;
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException {
		throw new UnsupportedOperationException( "array of maps is not yet supported" );
	}

	@Override
	public Object getArray(long l, int i) throws SQLException {
		throw new UnsupportedOperationException( "array of maps is not yet supported" );
	}

	@Override
	public Object getArray(long l, int i, Map<String, Class<?>> map) throws SQLException {
		throw new UnsupportedOperationException( "array of maps is not yet supported" );
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		throw new UnsupportedOperationException( "array of maps is not yet supported" );
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
		throw new UnsupportedOperationException( "array of maps is not yet supported" );
	}

	@Override
	public ResultSet getResultSet(long l, int i) throws SQLException {
		throw new UnsupportedOperationException( "array of maps is not yet supported" );
	}

	@Override
	public ResultSet getResultSet(long l, int i, Map<String, Class<?>> map) throws SQLException {
		throw new UnsupportedOperationException( "array of maps is not yet supported" );
	}

	@Override
	public void free() {
		objects = null;
	}
}
