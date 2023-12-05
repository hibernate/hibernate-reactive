/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.adaptor.impl;

import java.sql.Array;
import java.sql.ResultSet;
import java.util.Map;

import org.hibernate.type.descriptor.jdbc.JdbcType;

public class ArrayAdaptor implements Array {

	private final String baseTypeName;
	private final int jdbcTypeCode;
	private Object[] objects;

	public ArrayAdaptor(JdbcType elementJdbcType, Object[] objects) {
		this( elementJdbcType.getFriendlyName(), elementJdbcType.getJdbcTypeCode(), objects );
	}

	public ArrayAdaptor(String baseTypeName, Object[] objects) {
		this( baseTypeName, 0, objects );
	}

	private ArrayAdaptor(String baseTypeName, int jdbcTypeCode, Object[] objects) {
		this.baseTypeName = baseTypeName;
		this.jdbcTypeCode = jdbcTypeCode;
		this.objects = objects;
	}

	@Override
	public String getBaseTypeName() {
		return baseTypeName;
	}

	@Override
	public int getBaseType() {
		return jdbcTypeCode;
	}

	@Override
	public Object getArray() {
		return objects;
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) {
		throw new UnsupportedOperationException( "array of maps is not yet supported" );
	}

	@Override
	public Object getArray(long index, int count) {
		throw new UnsupportedOperationException( "array of maps is not yet supported" );
	}

	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map) {
		throw new UnsupportedOperationException( "array of maps is not yet supported" );
	}

	@Override
	public ResultSet getResultSet() {
		throw new UnsupportedOperationException( "array of maps is not yet supported" );
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> map) {
		throw new UnsupportedOperationException( "array of maps is not yet supported" );
	}

	@Override
	public ResultSet getResultSet(long index, int count) {
		throw new UnsupportedOperationException( "array of maps is not yet supported" );
	}

	@Override
	public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) {
		throw new UnsupportedOperationException( "array of maps is not yet supported" );
	}

	@Override
	public void free() {
		objects = null;
	}
}
