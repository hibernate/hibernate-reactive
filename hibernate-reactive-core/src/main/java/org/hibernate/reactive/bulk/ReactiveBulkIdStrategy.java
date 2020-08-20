/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.bulk;

import org.hibernate.boot.spi.MetadataBuildingOptions;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.dialect.DB297Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.connections.spi.JdbcConnectionAccess;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.hql.internal.ast.HqlSqlWalker;
import org.hibernate.hql.internal.ast.tree.AssignmentSpecification;
import org.hibernate.hql.internal.ast.tree.DeleteStatement;
import org.hibernate.hql.internal.ast.tree.FromElement;
import org.hibernate.hql.internal.ast.tree.UpdateStatement;
import org.hibernate.hql.spi.id.AbstractMultiTableBulkIdStrategyImpl;
import org.hibernate.hql.spi.id.AbstractTableBasedBulkIdHandler;
import org.hibernate.hql.spi.id.IdTableInfo;
import org.hibernate.hql.spi.id.MultiTableBulkIdStrategy;
import org.hibernate.hql.spi.id.local.IdTableInfoImpl;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Table;
import org.hibernate.param.ParameterSpecification;
import org.hibernate.persister.collection.AbstractCollectionPersister;
import org.hibernate.persister.entity.Queryable;
import org.hibernate.sql.Delete;
import org.hibernate.sql.SelectValues;
import org.hibernate.sql.Update;
import org.hibernate.type.CollectionType;
import org.hibernate.type.Type;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hibernate.hql.spi.id.persistent.Helper.SESSION_ID_COLUMN_NAME;
import static org.hibernate.reactive.bulk.SessionIdParameterSpecification.SESSION_ID;

/**
 * A reactive version of {@link AbstractMultiTableBulkIdStrategyImpl} used
 * for handling HQL update and delete queries that affect multiple tables.
 * <p>
 * Note that this class features hardcoded support for the three supported
 * databases.
 *
 * @author Gavin King
 */
public class ReactiveBulkIdStrategy
		extends AbstractMultiTableBulkIdStrategyImpl<IdTableInfoImpl, AbstractMultiTableBulkIdStrategyImpl.PreparationContext>
		implements MultiTableBulkIdStrategy {

	private final MetadataImplementor metadata;

	private boolean useSessionIdColumn() {
		return metadata.getDatabase().getDialect() instanceof DB297Dialect;
	}

	public ReactiveBulkIdStrategy(MetadataImplementor metadata) {
		super( new ReactiveIdTableSupport( metadata.getDatabase().getDialect() ) );
		this.metadata = metadata;
	}

	@Override
	protected void initialize(MetadataBuildingOptions buildingOptions, SessionFactoryOptions sessionFactoryOptions) {}

	@Override
	protected IdTableInfoImpl buildIdTableInfo(
			PersistentClass entityBinding,
			Table idTable,
			JdbcServices jdbcServices,
			MetadataImplementor metadata,
			PreparationContext context) {
		return new IdTableInfoImpl(
				jdbcServices.getJdbcEnvironment().getQualifiedObjectNameFormatter().format(
						idTable.getQualifiedTableName(),
						jdbcServices.getJdbcEnvironment().getDialect()
				),
				buildIdTableCreateStatement( idTable, jdbcServices, metadata ),
				buildIdTableDropStatement( idTable, jdbcServices )
		);
	}

	@Override
	public void release(JdbcServices jdbcServices, JdbcConnectionAccess connectionAccess) {}

	@Override
	public UpdateHandler buildUpdateHandler(SessionFactoryImplementor factory, HqlSqlWalker walker) {
		return new TableBasedUpdateHandlerImpl( factory, walker );
	}

	@Override
	public DeleteHandler buildDeleteHandler(SessionFactoryImplementor factory, HqlSqlWalker walker) {
		return new TableBasedDeleteHandlerImpl( factory, walker );
	}

	@Override
	protected void augmentIdTableDefinition(Table idTable) {
		if ( useSessionIdColumn() ) {
			Column sessionIdColumn = new Column( SESSION_ID_COLUMN_NAME );
			sessionIdColumn.setLength( 36 );
			sessionIdColumn.setNullable( false );
			SimpleValue value = new SimpleValue( metadata );
			value.setTypeName( "string" );
			sessionIdColumn.setValue( value );
			sessionIdColumn.setComment( "Hibernate Session identifier" );
			idTable.addColumn( sessionIdColumn );
		}
	}

	private class TableBasedUpdateHandlerImpl extends AbstractTableBasedBulkIdHandler
			implements MultiTableBulkIdStrategy.UpdateHandler, StatementsWithParameters {

		private final String[] statements;
		private final ParameterSpecification[][] parameterSpecifications;
		private final Queryable targetedPersister;

		@Override
		public Queryable getTargetedQueryable() {
			return targetedPersister;
		}

		@Override
		public String[] getStatements() {
			return statements;
		}

		@Override
		public ParameterSpecification[][] getParameterSpecifications() {
			return parameterSpecifications;
		}

		public TableBasedUpdateHandlerImpl(SessionFactoryImplementor factory, HqlSqlWalker walker) {
			super( factory, walker );

			ArrayList<String> statements = new ArrayList<>();
			ArrayList<ParameterSpecification[]> parameterSpecifications = new ArrayList<>();

			Dialect dialect = factory.getJdbcServices().getJdbcEnvironment().getDialect();

			UpdateStatement updateStatement = (UpdateStatement) walker.getAST();
			FromElement fromElement = updateStatement.getFromClause().getFromElement();
			IdTableInfoImpl tableInfo = getIdTableInfo( fromElement.getQueryable() );

			targetedPersister = fromElement.getQueryable();

			String bulkTargetAlias = fromElement.getTableAlias();
			ProcessedWhereClause processedWhereClause = processWhereClause( updateStatement.getWhereClause() );
			String idInsertSelect = generateIdInsertSelect( bulkTargetAlias, tableInfo, processedWhereClause );

			statements.add( tableInfo.getIdTableCreationStatement() );
			parameterSpecifications.add( new ParameterSpecification[0] );

			statements.add( idInsertSelect );
			ArrayList<ParameterSpecification> specifications = new ArrayList<>();
			if ( useSessionIdColumn() ) {
				specifications.add( SESSION_ID );
			}
			specifications.addAll( processedWhereClause.getIdSelectParameterSpecifications() );
			parameterSpecifications.add( specifications.toArray( new ParameterSpecification[0] ) );

			String[] tableNames = targetedPersister.getConstraintOrderedTableNameClosure();
			String[][] columnNames = targetedPersister.getContraintOrderedTableKeyColumnClosure();
			String idSubselect = generateIdSubselect( targetedPersister, tableInfo );
			for ( int tableIndex = 0; tableIndex < tableNames.length; tableIndex++ ) {
				boolean affected = false;
				List<ParameterSpecification> parameterList = new ArrayList<>();
				Update update = new Update( dialect )
						.setTableName( tableNames[tableIndex] )
						.setWhere( "(" + String.join( ", ", columnNames[tableIndex] ) + ") in (" + idSubselect + ")" );
				if ( factory().getSessionFactoryOptions().isCommentsEnabled() ) {
					update.setComment( "bulk update" );
				}
				@SuppressWarnings("unchecked")
				List<AssignmentSpecification> assignmentSpecifications = walker.getAssignmentSpecifications();
				for ( AssignmentSpecification assignmentSpecification : assignmentSpecifications ) {
					if ( assignmentSpecification.affectsTable( tableNames[tableIndex] ) ) {
						affected = true;
						update.appendAssignmentFragment( assignmentSpecification.getSqlAssignmentFragment() );
						if ( assignmentSpecification.getParameters() != null ) {
							Collections.addAll( parameterList, assignmentSpecification.getParameters() );
						}
					}
				}
				if ( affected ) {
					statements.add( update.toStatementString() );
					if ( useSessionIdColumn() ) {
						parameterList.add( SESSION_ID );
					}
					parameterSpecifications.add( parameterList.toArray( new ParameterSpecification[0] ) );
				}
			}

			if ( useSessionIdColumn() ) {
				Delete drop = new Delete()
						.setTableName( tableInfo.getQualifiedIdTableName() )
						.setWhere( SESSION_ID_COLUMN_NAME + "=?" );
				statements.add( drop.toStatementString() );
				parameterSpecifications.add( new ParameterSpecification[] { SESSION_ID } );
			}
			else {
				statements.add( tableInfo.getIdTableDropStatement() );
				parameterSpecifications.add( new ParameterSpecification[0] );
			}

			this.statements = statements.toArray( new String[0] );
			this.parameterSpecifications = parameterSpecifications.toArray( new ParameterSpecification[0][] );
		}

		@Override
		public String[] getSqlStatements() {
			return statements;
		}

		@Override
		public int execute(SharedSessionContractImplementor session, QueryParameters queryParameters) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected void prepareForUse(Queryable persister, SharedSessionContractImplementor session) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected void releaseFromUse(Queryable persister, SharedSessionContractImplementor session) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected void addAnyExtraIdSelectValues(SelectValues selectClause) {
			if ( useSessionIdColumn() ) {
				selectClause.addParameter( Types.CHAR, 36 );
			}
		}

		@Override
		protected String generateIdSubselect(Queryable persister, IdTableInfo idTableInfo) {
			String sql = super.generateIdSubselect( persister, idTableInfo );
			return useSessionIdColumn() ? sql + " where " + SESSION_ID_COLUMN_NAME + "=?" : sql;
		}
	}

	private class TableBasedDeleteHandlerImpl extends AbstractTableBasedBulkIdHandler
			implements MultiTableBulkIdStrategy.DeleteHandler, StatementsWithParameters {

		private final Queryable targetedPersister;

		private final ParameterSpecification[][] parameterSpecifications;
		private final String[] statements;

		@Override
		public String[] getStatements() {
			return statements;
		}

		@Override
		public ParameterSpecification[][] getParameterSpecifications() {
			return parameterSpecifications;
		}

		public TableBasedDeleteHandlerImpl(SessionFactoryImplementor factory, HqlSqlWalker walker) {
			super( factory, walker );

			ArrayList<String> statements = new ArrayList<>();
			ArrayList<ParameterSpecification[]> parameterSpecifications = new ArrayList<>();

			DeleteStatement deleteStatement = (DeleteStatement) walker.getAST();
			FromElement fromElement = deleteStatement.getFromClause().getFromElement();
			IdTableInfoImpl tableInfo = getIdTableInfo( fromElement.getQueryable() );

			targetedPersister = fromElement.getQueryable();

			String bulkTargetAlias = fromElement.getTableAlias();
			ProcessedWhereClause processedWhereClause = processWhereClause( deleteStatement.getWhereClause() );
			String idInsertSelect = generateIdInsertSelect( bulkTargetAlias, tableInfo, processedWhereClause );

			statements.add( tableInfo.getIdTableCreationStatement() );
			parameterSpecifications.add( new ParameterSpecification[0] );

			statements.add( idInsertSelect );
			ArrayList<ParameterSpecification> specifications = new ArrayList<>();
			if ( useSessionIdColumn() ) {
				specifications.add( SESSION_ID );
			}
			specifications.addAll( processedWhereClause.getIdSelectParameterSpecifications() );
			parameterSpecifications.add( specifications.toArray( new ParameterSpecification[0] ) );

			// If many-to-many, delete the FK row in the collection table.
			// This partially overlaps with DeleteExecutor, but it instead
			// uses the temp table in the idSubselect.
			for ( Type type : targetedPersister.getPropertyTypes() ) {
				if ( type.isCollectionType() ) {
					CollectionType cType = (CollectionType) type;
					AbstractCollectionPersister cPersister = (AbstractCollectionPersister)
							factory.getMetamodel().collectionPersister( cType.getRole() );
					if ( cPersister.isManyToMany() ) {
						Delete delete = new Delete()
								.setTableName( cPersister.getTableName() )
								.setWhere( "(" + String.join( ", ", cPersister.getKeyColumnNames() )
										+ ") in (" + generateIdSubselect( targetedPersister, cPersister, tableInfo ) + ")" );
						if ( factory().getSessionFactoryOptions().isCommentsEnabled() ) {
							delete.setComment("bulk delete - m2m join table cleanup");
						}
						statements.add( delete.toStatementString() );
						parameterSpecifications.add( new ParameterSpecification[0] );
					}
				}
			}

			String idSubselect = generateIdSubselect( targetedPersister, tableInfo );
			String[] tableNames = targetedPersister.getConstraintOrderedTableNameClosure();
			String[][] columnNames = targetedPersister.getContraintOrderedTableKeyColumnClosure();
			for ( int i = 0; i < tableNames.length; i++ ) {
				Delete delete = new Delete()
						.setTableName( tableNames[i] )
						.setWhere( "(" + String.join( ", ", columnNames[i]) + ") in (" + idSubselect + ")" );
				if ( factory().getSessionFactoryOptions().isCommentsEnabled() ) {
					delete.setComment("bulk delete");
				}
				statements.add( delete.toStatementString() );
				parameterSpecifications.add( useSessionIdColumn() ? new ParameterSpecification[] { SESSION_ID } : new ParameterSpecification[0] );
			}

			if ( useSessionIdColumn() ) {
				Delete drop = new Delete()
						.setTableName( tableInfo.getQualifiedIdTableName() )
						.setWhere( SESSION_ID_COLUMN_NAME + "=?" );
				statements.add( drop.toStatementString() );
				parameterSpecifications.add( new ParameterSpecification[] { SESSION_ID } );
			}
			else {
				statements.add( tableInfo.getIdTableDropStatement() );
				parameterSpecifications.add( new ParameterSpecification[0] );
			}

			this.statements = statements.toArray( new String[0] );
			this.parameterSpecifications = parameterSpecifications.toArray( new ParameterSpecification[0][] );
		}

		@Override
		public Queryable getTargetedQueryable() {
			return targetedPersister;
		}

		@Override
		public String[] getSqlStatements() {
			return statements;
		}

		@Override
		public int execute(SharedSessionContractImplementor session, QueryParameters queryParameters) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected void addAnyExtraIdSelectValues(SelectValues selectClause) {
			if ( useSessionIdColumn() ) {
				selectClause.addParameter( Types.VARCHAR, 36 );
			}
		}

		@Override
		protected String generateIdSubselect(Queryable persister, IdTableInfo idTableInfo) {
			String sql = super.generateIdSubselect( persister, idTableInfo );
			return useSessionIdColumn() ? sql + " where " + SESSION_ID_COLUMN_NAME + "=?" : sql;
		}

		@Override
		protected String generateIdSubselect(Queryable persister, AbstractCollectionPersister collectionPersister,
											 IdTableInfo idTableInfo) {
			String sql = super.generateIdSubselect( persister, collectionPersister, idTableInfo );
			return useSessionIdColumn() ? sql + " where " + SESSION_ID_COLUMN_NAME + "=?" : sql;
		}
	}

}
