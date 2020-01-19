/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.sql.impl;

import org.hibernate.dialect.Dialect;
import org.hibernate.type.LiteralType;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * An {@link org.hibernate.sql.Update} that generates
 * SQL with the database-native bind variable syntax.
 */
public class Update extends org.hibernate.sql.Update {

	private String tableName;
	private String versionColumnName;
	private String where;
	private String assignments;
	private String comment;
	private final Supplier<String> nextParameter;

	private Map<String,String> columns = new LinkedHashMap<>();
	private Map<String,Supplier<String>> primaryKeyColumns = new LinkedHashMap<>();
	private Map<String,Supplier<String>> whereColumns = new LinkedHashMap<>();
	
	private Dialect dialect;

	public Update(Dialect dialect, Supplier<String> nextParameter) {
		super(dialect);
		this.dialect = dialect;
		this.nextParameter = nextParameter;
	}

	public Update(Dialect dialect) {
		super(dialect);
		this.dialect = dialect;
		nextParameter = () -> "?";
	}

	public String getTableName() {
		return tableName;
	}

	public Update appendAssignmentFragment(String fragment) {
		if ( assignments == null ) {
			assignments = fragment;
		}
		else {
			assignments += ", " + fragment;
		}
		return this;
	}

	public Update setTableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public Update setPrimaryKeyColumnNames(String[] columnNames) {
		this.primaryKeyColumns.clear();
		addPrimaryKeyColumns(columnNames);
		return this;
	}	
	
	public Update addPrimaryKeyColumns(String[] columnNames) {
		for ( String columnName : columnNames ) {
			addPrimaryKeyColumn( columnName, "?" );
		}
		return this;
	}
	
	public Update addPrimaryKeyColumns(String[] columnNames, boolean[] includeColumns, String[] valueExpressions) {
		for ( int i=0; i<columnNames.length; i++ ) {
			if( includeColumns[i] ) {
				addPrimaryKeyColumn( columnNames[i], valueExpressions[i] );
			}
		}
		return this;
	}
	
	public Update addPrimaryKeyColumns(String[] columnNames, String[] valueExpressions) {
		for ( int i=0; i<columnNames.length; i++ ) {
			addPrimaryKeyColumn( columnNames[i], valueExpressions[i] );
		}
		return this;
	}	

	public Update addPrimaryKeyColumn(String columnName, String valueExpression) {
		if ( "?".equals(valueExpression) ) {
			this.primaryKeyColumns.put(columnName, nextParameter);
		}
		else {
			this.primaryKeyColumns.put(columnName, () -> valueExpression);
		}
		return this;
	}
	
	public Update setVersionColumnName(String versionColumnName) {
		this.versionColumnName = versionColumnName;
		return this;
	}


	public Update setComment(String comment) {
		this.comment = comment;
		return this;
	}
	
	public Update addColumns(String[] columnNames) {
		for ( String columnName : columnNames ) {
			addColumn( columnName );
		}
		return this;
	}

	public Update addColumns(String[] columnNames, boolean[] updateable, String[] valueExpressions) {
		for ( int i=0; i<columnNames.length; i++ ) {
			if ( updateable[i] ) {
				addColumn( columnNames[i], valueExpressions[i] );
			}
		}
		return this;
	}

	public Update addColumns(String[] columnNames, String valueExpression) {
		for ( String columnName : columnNames ) {
			addColumn( columnName, valueExpression );
		}
		return this;
	}

	public Update addColumn(String columnName) {
		return addColumn( columnName, "?" );
	}

	public Update addColumn(String columnName, String valueExpression) {
		if ( "?".equals(valueExpression) ) {
			valueExpression = nextParameter.get();
		}
		columns.put(columnName, valueExpression);
		return this;
	}

	public Update addColumn(String columnName, Object value, LiteralType type) throws Exception {
		return addColumn( columnName, type.objectToSQLString(value, dialect) );
	}

	public Update addWhereColumns(String[] columnNames) {
		for ( String columnName : columnNames ) {
			addWhereColumn( columnName );
		}
		return this;
	}

	public Update addWhereColumns(String[] columnNames, String valueExpression) {
		for ( String columnName : columnNames ) {
			addWhereColumn( columnName, valueExpression );
		}
		return this;
	}

	public Update addWhereColumn(String columnName) {
		return addWhereColumn(columnName, "?");
	}

	public Update addWhereColumn(String columnName, String valueExpression) {
		if ( "?".equals(valueExpression) ) {
			whereColumns.put(columnName, () -> "=" + nextParameter.get());
		}
		else {
			whereColumns.put(columnName, () -> valueExpression);
		}
		return this;
	}

	public Update setWhere(String where) {
		this.where=where;
		return this;
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
			buf.append( e.getKey() ).append( '=' ).append( e.getValue() );
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
		Iterator<Map.Entry<String,Supplier<String>>> iter2 = primaryKeyColumns.entrySet().iterator();
		while ( iter2.hasNext() ) {
			Map.Entry<String,Supplier<String>> e = iter2.next();
			buf.append( e.getKey() ).append( '=' ).append( e.getValue().get() );
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
			final Map.Entry<String,Supplier<String>> e = iter2.next();
			if ( conditionsAppended ) {
				buf.append( " and " );
			}
			buf.append( e.getKey() ).append( e.getValue().get() );
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
}
