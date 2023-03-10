/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockOptions;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.loader.ast.internal.LoaderSqlAstCreationState;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.sqm.ComparisonOperator;
import org.hibernate.query.sqm.sql.FromClauseIndex;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.spi.SqlAliasBaseManager;
import org.hibernate.sql.ast.spi.SqlExpressionResolver;
import org.hibernate.sql.ast.tree.expression.ColumnReference;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.expression.QueryLiteral;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.ast.tree.from.TableReference;
import org.hibernate.sql.ast.tree.predicate.ComparisonPredicate;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.BaseExecutionContext;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.internal.JdbcParameterImpl;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.FetchParent;
import org.hibernate.sql.results.graph.internal.ImmutableFetchList;
import org.hibernate.sql.results.internal.RowTransformerDatabaseSnapshotImpl;
import org.hibernate.type.StandardBasicTypes;

import org.jboss.logging.Logger;

/**
 * @see org.hibernate.loader.ast.internal.DatabaseSnapshotExecutor
 */
class DatabaseSnapshotExecutor {
	private static final Logger log = Logger.getLogger( DatabaseSnapshotExecutor.class );

	private final EntityMappingType entityDescriptor;

	private final JdbcOperationQuerySelect jdbcSelect;
	private final List<JdbcParameter> jdbcParameters;

	DatabaseSnapshotExecutor(
			EntityMappingType entityDescriptor,
			SessionFactoryImplementor sessionFactory) {
		this.entityDescriptor = entityDescriptor;
		this.jdbcParameters = new ArrayList<>(
				entityDescriptor.getIdentifierMapping().getJdbcTypeCount()
		);

		final QuerySpec rootQuerySpec = new QuerySpec( true );

		final SqlAliasBaseManager sqlAliasBaseManager = new SqlAliasBaseManager();

		final LoaderSqlAstCreationState state = new LoaderSqlAstCreationState(
				rootQuerySpec,
				sqlAliasBaseManager,
				new FromClauseIndex( null ),
				LockOptions.NONE,
				DatabaseSnapshotExecutor::visitEmptyFetchList,
				true,
				LoadQueryInfluencers.NONE,
				sessionFactory
		);

		final NavigablePath rootPath = new NavigablePath( entityDescriptor.getEntityName() );

		final TableGroup rootTableGroup = entityDescriptor.createRootTableGroup(
				true,
				rootPath,
				null,
				null,
				() -> rootQuerySpec::applyPredicate,
				state
		);
		rootQuerySpec.getFromClause().addRoot( rootTableGroup );
		state.getFromClauseAccess().registerTableGroup( rootPath, rootTableGroup );

		// We produce the same state array as if we were creating an entity snapshot
		final List<DomainResult<?>> domainResults = new ArrayList<>();

		final SqlExpressionResolver sqlExpressionResolver = state.getSqlExpressionResolver();

		// We just need a literal to have a result set
		domainResults.add(
				new QueryLiteral<>(
						null,
						sessionFactory.getTypeConfiguration()
								.getBasicTypeRegistry()
								.resolve( StandardBasicTypes.INTEGER )
				).createDomainResult( null, state )
		);
		final NavigablePath idNavigablePath = rootPath.append( entityDescriptor.getIdentifierMapping().getNavigableRole().getNavigableName() );
		entityDescriptor.getIdentifierMapping().forEachSelectable(
				(columnIndex, selection) -> {
					final TableReference tableReference = rootTableGroup.resolveTableReference(
							idNavigablePath,
							selection.getContainingTableExpression()
					);

					final JdbcParameter jdbcParameter = new JdbcParameterImpl( selection.getJdbcMapping() );
					jdbcParameters.add( jdbcParameter );

					final ColumnReference columnReference = (ColumnReference) sqlExpressionResolver
							.resolveSqlExpression( tableReference, selection );

					rootQuerySpec.applyPredicate(
							new ComparisonPredicate(
									columnReference,
									ComparisonOperator.EQUAL,
									jdbcParameter
							)
					);
				}
		);


		entityDescriptor.forEachAttributeMapping(
				attributeMapping -> {
					final NavigablePath navigablePath = rootPath.append( attributeMapping.getAttributeName() );
					domainResults.add(
							attributeMapping.createSnapshotDomainResult(
									navigablePath,
									rootTableGroup,
									null,
									state
							)
					);
				}
		);

		final SelectStatement selectStatement = new SelectStatement( rootQuerySpec, domainResults );

		final JdbcServices jdbcServices = sessionFactory.getJdbcServices();
		final JdbcEnvironment jdbcEnvironment = jdbcServices.getJdbcEnvironment();
		final SqlAstTranslatorFactory sqlAstTranslatorFactory = jdbcEnvironment.getSqlAstTranslatorFactory();

		this.jdbcSelect = sqlAstTranslatorFactory.buildSelectTranslator( sessionFactory, selectStatement )
				.translate( null, QueryOptions.NONE );
	}

	private static ImmutableFetchList visitEmptyFetchList(FetchParent fetchParent, LoaderSqlAstCreationState creationState) {
		return ImmutableFetchList.EMPTY;
	}

	CompletionStage<Object[]> loadDatabaseSnapshot(Object id, SharedSessionContractImplementor session) {
		if ( log.isTraceEnabled() ) {
			log.tracef( "Getting current persistent state for `%s#%s`", entityDescriptor.getEntityName(), id );
		}

		final JdbcParameterBindings jdbcParameterBindings = new JdbcParameterBindingsImpl(
				entityDescriptor.getIdentifierMapping().getJdbcTypeCount()
		);

		int offset = jdbcParameterBindings.registerParametersForEachJdbcValue(
				id,
				entityDescriptor.getIdentifierMapping(),
				jdbcParameters,
				session
		);
		assert offset == jdbcParameters.size();

		// FIXME: use JdbcServices
		return StandardReactiveSelectExecutor.INSTANCE
				.list( jdbcSelect, jdbcParameterBindings, new BaseExecutionContext( session ), RowTransformerDatabaseSnapshotImpl.instance(), ReactiveListResultsConsumer.UniqueSemantic.FILTER  )
				.thenApply( list -> {
					assert list != null;
					final int size = list.size();
					assert size <= 1;

					if ( size == 0 ) {
						return null;
					}

					final Object[] entitySnapshot = (Object[]) list.get( 0 );
					// The result of this method is treated like the entity state array which doesn't include the id
					// So we must exclude it from the array
					if ( entitySnapshot.length == 1 ) {
						return ArrayHelper.EMPTY_OBJECT_ARRAY;
					}

					final Object[] state = new Object[entitySnapshot.length - 1];
					System.arraycopy( entitySnapshot, 1, state, 0, state.length );
					return state;
				} );
	}
}
