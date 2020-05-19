/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.reactive.sql.impl;

import org.hibernate.dialect.Dialect;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

/**
 * An {@link org.hibernate.sql.Update} that generates
 * SQL with the database-native bind variable syntax.
 */
public class Update extends org.hibernate.sql.Update {

	private final Supplier<String> nextParameter;

	public Update(Dialect dialect, Supplier<String> nextParameter) {
		super(dialect);
		this.nextParameter = nextParameter;
	}

	public String toStatementString() {
		StringBuilder buf = new StringBuilder( (columns.size() * 15) + tableName.length() + 10 );
		if ( comment!=null ) {
			buf.append( "/* " ).append( comment ).append( " */ " );
		}
		buf.append( "update " ).append( tableName ).append( " set " );
		boolean assignmentsAppended = false;
		Iterator<Map.Entry<String,String>> iter = columns.entrySet().iterator();
		while ( iter.hasNext() ) {
			Map.Entry<String,String> e = iter.next();
			buf.append( e.getKey() ).append( '=' ).append( value( e.getValue() ) );
			if ( iter.hasNext() ) {
				buf.append( ", " );
			}
			assignmentsAppended = true;
		}
		if ( assignments != null ) {
			if ( assignmentsAppended ) {
				buf.append( ", " );
			}
			buf.append( assignments );
		}

		boolean conditionsAppended = false;
		if ( !primaryKeyColumns.isEmpty() || where != null || !whereColumns.isEmpty() || versionColumnName != null ) {
			buf.append( " where " );
		}
		Iterator<Map.Entry<String,String>> iter2 = primaryKeyColumns.entrySet().iterator();
		while ( iter2.hasNext() ) {
			Map.Entry<String,String> e = iter2.next();
			buf.append( e.getKey() ).append( '=' ).append( value( e.getValue() ) );
			if ( iter2.hasNext() ) {
				buf.append( " and " );
			}
			conditionsAppended = true;
		}
		if ( where != null ) {
			if ( conditionsAppended ) {
				buf.append( " and " );
			}
			buf.append( where );
			conditionsAppended = true;
		}
		iter2 = whereColumns.entrySet().iterator();
		while ( iter2.hasNext() ) {
			final Map.Entry<String,String> e = iter2.next();
			if ( conditionsAppended ) {
				buf.append( " and " );
			}
			buf.append( e.getKey() ).append( value( e.getValue() ) );
			conditionsAppended = true;
		}
		if ( versionColumnName != null ) {
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
