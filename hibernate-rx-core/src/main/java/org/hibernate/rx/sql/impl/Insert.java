/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.sql.impl;

import org.hibernate.MappingException;
import org.hibernate.dialect.Dialect;
import org.hibernate.type.LiteralType;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * An {@link org.hibernate.sql.Insert} that generates
 * SQL with the database-native bind variable syntax.
 */
public class Insert extends org.hibernate.sql.Insert {
	private Dialect dialect;
	private String tableName;
	private String comment;
	private Map<String,String> columns = new LinkedHashMap<>();
	private final Supplier<String> nextParameter;

	public Insert(Dialect dialect) {
		super(dialect);
		this.dialect = dialect;
		nextParameter = () -> "?";
	}

	public Insert(Dialect dialect, Supplier<String> nextParameter) {
		super(dialect);
		this.dialect = dialect;
		this.nextParameter = nextParameter;
	}

	protected Dialect getDialect() {
		return dialect;
	}

	public Insert setComment(String comment) {
		this.comment = comment;
		return this;
	}

	public Insert addColumn(String columnName) {
		return addColumn( columnName, nextParameter.get() );
	}

	public Insert addColumns(String[] columnNames) {
		for ( String columnName : columnNames ) {
			addColumn( columnName );
		}
		return this;
	}

	public Insert addColumns(String[] columnNames, boolean[] insertable) {
		for ( int i=0; i<columnNames.length; i++ ) {
			if ( insertable[i] ) {
				addColumn( columnNames[i] );
			}
		}
		return this;
	}

	public Insert addColumns(String[] columnNames, boolean[] insertable, String[] valueExpressions) {
		for ( int i=0; i<columnNames.length; i++ ) {
			if ( insertable[i] ) {
				addColumn( columnNames[i], valueExpressions[i] );
			}
		}
		return this;
	}

	public Insert addColumn(String columnName, String valueExpression) {
		if ( "?".equals(valueExpression) ) {
			valueExpression = nextParameter.get();
		}
		columns.put( columnName, valueExpression );
		return this;
	}

	public Insert addColumn(String columnName, Object value, LiteralType type) throws Exception {
		return addColumn( columnName, type.objectToSQLString( value, dialect ) );
	}

	public Insert addIdentityColumn(String columnName) {
		String value = dialect.getIdentityColumnSupport().getIdentityInsertString();
		if ( value != null ) {
			addColumn( columnName, value );
		}
		return this;
	}

	public Insert setTableName(String tableName) {
		this.tableName = tableName;
		return this;
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
				buf.append( iter.next() );
				if ( iter.hasNext() ) {
					buf.append( ", " );
				}
			}
			buf.append(')');
		}
		return buf.toString();
	}
}
