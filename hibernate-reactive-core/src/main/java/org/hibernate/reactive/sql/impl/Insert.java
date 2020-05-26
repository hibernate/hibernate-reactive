/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.impl;

import org.hibernate.MappingException;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;

import java.util.Iterator;
import java.util.function.Supplier;

import static org.hibernate.reactive.sql.impl.Parameters.createDialectParameterGenerator;

/**
 * An {@link org.hibernate.sql.Insert} that generates
 * SQL with the database-native bind variable syntax.
 */
public class Insert extends org.hibernate.sql.Insert {

	private Dialect dialect;
	private final Supplier<String> nextParameter;

	public Insert(SessionFactoryImplementor factory) {
		this( factory.getJdbcServices().getDialect() );
	}

	public Insert(Dialect dialect) {
		this( dialect, createDialectParameterGenerator( dialect ) );
	}

	public Insert(Dialect dialect, Supplier<String> nextParameter) {
		super(dialect);
		this.dialect = dialect;
		this.nextParameter = nextParameter;
	}

	public String toStatementString() {
		StringBuilder buf = new StringBuilder( columns.size()*15 + tableName.length() + 10 );
		if ( comment != null ) {
			buf.append( "/* " ).append( comment ).append( " */ " );
		}
		buf.append("insert into ")
			.append(tableName);
		if ( columns.size()==0 ) {
			if ( dialect.supportsNoColumnsInsert() ) {
				buf.append( ' ' ).append( dialect.getNoColumnsInsertString() );
			}
			else {
				throw new MappingException(
						String.format(
								"The INSERT statement for table [%s] contains no column, and this is not supported by [%s]",
								tableName,
								dialect
						)
				);
			}
		}
		else {
			buf.append(" (");
			Iterator<String> iter = columns.keySet().iterator();
			while ( iter.hasNext() ) {
				buf.append( iter.next() );
				if ( iter.hasNext() ) {
					buf.append( ", " );
				}
			}
			buf.append(") values (");
			iter = columns.values().iterator();
			while ( iter.hasNext() ) {
				buf.append( value( iter.next() ) );
				if ( iter.hasNext() ) {
					buf.append( ", " );
				}
			}
			buf.append(')');
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
