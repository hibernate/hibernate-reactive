/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;

public class BigDecimalAsString implements UserType<BigDecimal> {

	@Override
	public int getSqlType() {
		return Types.VARCHAR;
	}

	@Override
	public Class<BigDecimal> returnedClass() {
		return BigDecimal.class;
	}

	@Override
	public boolean equals(BigDecimal x, BigDecimal y) {
		return Objects.equals( x, y );
	}

	@Override
	public int hashCode(BigDecimal x) {
		return Objects.hashCode( x );
	}

	@Override
	public BigDecimal nullSafeGet(ResultSet rs, int position, SharedSessionContractImplementor session, Object owner) throws SQLException {
		String string = rs.getString( position );
		return string == null || rs.wasNull() ? null : new BigDecimal( string );
	}

	@Override
	public void nullSafeSet(PreparedStatement st, BigDecimal value, int index, SharedSessionContractImplementor session) throws SQLException {
		if ( value == null ) {
			st.setNull( index, Types.VARCHAR );
		}
		else {
			st.setString( index, value.toString() );
		}
	}

	@Override
	public BigDecimal deepCopy(BigDecimal value) {
		return value;
	}

	@Override
	public boolean isMutable() {
		return false;
	}

	@Override
	public Serializable disassemble(BigDecimal value) {
		return value;
	}

	@Override
	public BigDecimal assemble(Serializable cached, Object owner) {
		return (BigDecimal) cached;
	}

	@Override
	public BigDecimal replace(BigDecimal detached, BigDecimal managed, Object owner) {
		return detached;
	}
}
