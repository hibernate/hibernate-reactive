/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.sql.impl;

import org.hibernate.MappingException;
import org.hibernate.dialect.Dialect;

import java.util.Iterator;
import java.util.function.Supplier;

/**
 * An {@link org.hibernate.sql.Insert} that generates
 * SQL with the database-native bind variable syntax.
 */
public class Insert extends org.hibernate.sql.Insert {

	private Dialect dialect;
	private final Supplier<String> nextParameter;

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
