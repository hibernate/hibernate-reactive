/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.impl;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

import static org.hibernate.reactive.sql.impl.Parameters.createDialectParameterGenerator;

/**
 * A {@link org.hibernate.sql.Delete} that generates
 * SQL with the database-native bind variable syntax.
 */
public class Delete extends org.hibernate.sql.Delete {

	private final Supplier<String> nextParameter;

	public Delete(SessionFactoryImplementor factory) {
		this( factory.getJdbcServices().getDialect() );
	}

	public Delete(Dialect dialect) {
		this( createDialectParameterGenerator( dialect ) );
	}

	public Delete(Supplier<String> nextParameter) {
		this.nextParameter = nextParameter;
	}

	public String toStatementString() {
		StringBuilder buf = new StringBuilder( tableName.length() + 10 );
		if ( comment!=null ) {
			buf.append( "/* " ).append(comment).append( " */ " );
		}
		buf.append( "delete from " ).append(tableName);
		if ( where != null || !primaryKeyColumns.isEmpty() || versionColumnName != null ) {
			buf.append( " where " );
		}
		boolean conditionsAppended = false;
		Iterator<Map.Entry<String,String>> iter = primaryKeyColumns.entrySet().iterator();
		while ( iter.hasNext() ) {
			Map.Entry<String,String> e =  iter.next();
			buf.append( e.getKey() ).append( '=' ).append( value( e.getValue() ) );
			if ( iter.hasNext() ) {
				buf.append( " and " );
			}
			conditionsAppended = true;
		}
		if ( where!=null ) {
			if ( conditionsAppended ) {
				buf.append( " and " );
			}
			buf.append( where );
			conditionsAppended = true;
		}
		if ( versionColumnName!=null ) {
			if ( conditionsAppended ) {
				buf.append( " and " );
			}
			buf.append( versionColumnName ).append( "=" ).append( nextParameter.get() );
		}
		return buf.toString();
	}

	private String value(String val) {
		switch (val) {
			case "?":
				return nextParameter.get();
			case "=?":
			case "= ?":
			case " = ?":
				return "=" + nextParameter.get();
			default:
				return val;
		}
	}

}
