package org.hibernate.rx.sql;

import java.util.LinkedHashMap;
import java.util.Map;

import org.hibernate.dialect.Dialect;
import org.hibernate.type.LiteralType;

/**
 * @see org.hibernate.sql.Update
 */
public class Update extends org.hibernate.sql.Update {

	private String tableName;
	private String versionColumnName;
	private String where;
	private String assignments;
	private String comment;

	private Map<String, String> primaryKeyColumns = new LinkedHashMap();
	private Map<String, String> columns = new LinkedHashMap();
	private Map<String, String> whereColumns = new LinkedHashMap();

	private Dialect dialect;

	public Update(Dialect dialect) {
		super( dialect );
	}

	public String getTableName() {
		return tableName;
	}

	@Override
	public Update setTableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	@Override
	public Update appendAssignmentFragment(String fragment) {
		if ( assignments == null ) {
			assignments = fragment;
		}
		else {
			assignments += ", " + fragment;
		}
		return this;
	}

	@Override
	public Update setPrimaryKeyColumnNames(String[] columnNames) {
		this.primaryKeyColumns.clear();
		addPrimaryKeyColumns( columnNames );
		return this;
	}

	@Override
	public Update addPrimaryKeyColumn(String columnName, String valueExpression) {
		this.primaryKeyColumns.put( columnName, valueExpression );
		return this;
	}

	@Override
	public Update setVersionColumnName(String versionColumnName) {
		this.versionColumnName = versionColumnName;
		return this;
	}

	@Override
	public Update setComment(String comment) {
		this.comment = comment;
		return this;
	}

	@Override
	public Update addColumn(String columnName, String valueExpression) {
		columns.put( columnName, valueExpression );
		return this;
	}

	@Override
	public Update addWhereColumn(String columnName, String valueExpression) {
		whereColumns.put( columnName, valueExpression );
		return this;
	}

	@Override
	public Update setWhere(String where) {
		this.where = where;
		return this;
	}

	@Override
	public String toStatementString() {
		int paramIndex = 0;
		StringBuilder buf = new StringBuilder( ( columns.size() * 15 ) + tableName.length() + 10 );
		if ( comment != null ) {
			buf.append( "/* " ).append( comment ).append( " */ " );
		}
		buf.append( "update " ).append( tableName ).append( " set " );

		StringBuilder setBuilder = new StringBuilder();
		for ( Map.Entry<String, String> entry : columns.entrySet() ) {
			setBuilder.append( entry.getKey() ).append( '=' );
			if ( entry.getValue().equals( "?" ) ) {
				setBuilder.append( "$" );
				setBuilder.append( ++paramIndex );
			}
			else {
				setBuilder.append( entry.getValue() );
			}
			setBuilder.append( ", " );
		}
		if ( assignments != null ) {
			setBuilder.append( assignments );
			setBuilder.append( ", " );
		}
		if ( setBuilder.length() > 0 ) {
			// Remove the comma at the end
			buf.append( setBuilder.substring( 0, setBuilder.length() - 2 ) );
		}

		StringBuilder whereBuilder = new StringBuilder();
		for ( Map.Entry<String, String> entry : primaryKeyColumns.entrySet() ) {
			whereBuilder.append( entry.getKey() ).append( '=' );
			if ( entry.getValue().equals( "?" ) ) {
				whereBuilder.append( "$" );
				whereBuilder.append( ++paramIndex );
			}
			else {
				whereBuilder.append( entry.getValue() );
			}
			whereBuilder.append( " and " );
		}

		if ( where != null ) {
			whereBuilder.append( where );
			whereBuilder.append( " and " );
		}

		for ( Map.Entry<String, String> entry : whereColumns.entrySet() ) {
			whereBuilder.append( entry.getKey() );
			if ( entry.getValue().equals( "=?" ) ) {
				whereBuilder.append( "=$" );
				whereBuilder.append( ++paramIndex );
			}
			else {
				whereBuilder.append( entry.getValue() );
			}
			whereBuilder.append( " and " );
		}

		if ( versionColumnName != null ) {
			whereBuilder.append( versionColumnName ).append( "=$" ).append( ++paramIndex );
			whereBuilder.append( " and " );
		}

		if ( whereBuilder.length() > 0 ) {
			buf.append( " where " );
			// Remove the " and " at the end
			buf.append( whereBuilder.substring( 0, whereBuilder.length() - 5 ) );
		}
		return buf.toString();
	}
}
