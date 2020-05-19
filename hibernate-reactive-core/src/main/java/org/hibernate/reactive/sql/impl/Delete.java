/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.reactive.sql.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A {@link org.hibernate.sql.Delete} that generates
 * SQL with the database-native bind variable syntax.
 */
public class Delete extends org.hibernate.sql.Delete {

	private final Supplier<String> nextParameter;

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
