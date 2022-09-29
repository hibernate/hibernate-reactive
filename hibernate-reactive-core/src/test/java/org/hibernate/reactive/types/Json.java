/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import io.vertx.core.json.JsonObject;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;

public class Json implements UserType<JsonObject> {

	@Override
	public int getSqlType() {
		return Types.OTHER;
	}

	@Override
	public Class<JsonObject> returnedClass() {
		return JsonObject.class;
	}

	@Override
	public boolean equals(JsonObject x, JsonObject y) throws HibernateException {
		return Objects.equals( x, y );
	}

	@Override
	public int hashCode(JsonObject x) throws HibernateException {
		return Objects.hashCode( x );
	}

	@Override
	public JsonObject nullSafeGet(ResultSet rs, int position, SharedSessionContractImplementor session, Object owner) throws SQLException {
		Object rsObject = rs.getObject( position );
		// Currently Vertx does not return JsonObject type from MariaDb so adding type check to convert the String value
		if ( rsObject instanceof String ) {
			return new JsonObject( rsObject.toString() );
		}
		return (JsonObject) rsObject;
	}

	@Override
	public void nullSafeSet(PreparedStatement st, JsonObject value, int index, SharedSessionContractImplementor session) throws HibernateException, SQLException {
		if ( value == null ) {
			st.setNull( index, Types.OTHER );
		}
		else {
			st.setObject( index, value );
		}
	}

	@Override
	public JsonObject deepCopy(JsonObject value) {
		return value == null ? null : ( (JsonObject) value ).copy();
	}

	@Override
	public boolean isMutable() {
		return true;
	}

	@Override
	public Serializable disassemble(JsonObject value) throws HibernateException {
		throw new UnsupportedOperationException();
	}

	@Override
	public JsonObject assemble(Serializable cached, Object owner) throws HibernateException {
		throw new UnsupportedOperationException();
	}

	@Override
	public JsonObject replace(JsonObject detached, JsonObject managed, Object owner) {
		throw new UnsupportedOperationException();
	}
}
