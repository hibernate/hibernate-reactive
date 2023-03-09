/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.lang.invoke.MethodHandles;;
import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.hibernate.HibernateException;
import org.hibernate.LockOptions;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.internal.LoaderSqlAstCreationState;
import org.hibernate.loader.ast.internal.SimpleNaturalIdLoader;
import org.hibernate.loader.ast.spi.NaturalIdLoadOptions;
import org.hibernate.metamodel.mapping.EntityIdentifierMapping;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.internal.SimpleNaturalIdMapping;
import org.hibernate.query.internal.SimpleQueryOptions;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.loader.ast.spi.ReactiveNaturalIdLoader;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.spi.SimpleFromClauseAccessImpl;
import org.hibernate.sql.ast.spi.SqlAliasBaseManager;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.BaseExecutionContext;
import org.hibernate.sql.exec.internal.CallbackImpl;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.Callback;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.FetchParent;
import org.hibernate.sql.results.graph.Fetchable;
import org.hibernate.sql.results.graph.FetchableContainer;
import org.hibernate.sql.results.graph.internal.ImmutableFetchList;
import org.hibernate.stat.spi.StatisticsImplementor;

/**
 * @see SimpleNaturalIdLoader
 */
public class ReactiveSimpleNaturalIdLoader<T> extends SimpleNaturalIdLoader<CompletionStage<T>>
		implements ReactiveNaturalIdLoader<T> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final ReactiveNaturalIdLoaderDelegate delegate;

	public ReactiveSimpleNaturalIdLoader(SimpleNaturalIdMapping naturalIdMapping, EntityMappingType entityDescriptor) {
		super( naturalIdMapping, entityDescriptor );
		delegate = new ReactiveNaturalIdLoaderDelegate( naturalIdMapping, entityDescriptor ) {
			@Override
			protected void applyNaturalIdRestriction(
					Object bindValue,
					TableGroup rootTableGroup,
					Consumer consumer,
					BiConsumer jdbcParameterConsumer,
					LoaderSqlAstCreationState sqlAstCreationState,
					SharedSessionContractImplementor session) {
				ReactiveSimpleNaturalIdLoader.this.applyNaturalIdRestriction(
						bindValue,
						rootTableGroup,
						consumer,
						jdbcParameterConsumer,
						sqlAstCreationState,
						session
				);
			}
		};;
	}

	/**
	 * @see org.hibernate.loader.ast.internal.AbstractNaturalIdLoader#resolveIdToNaturalId(Object, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Object> reactiveResolveIdToNaturalId(Object id, SharedSessionContractImplementor session) {
		return delegate.reactiveResolveIdToNaturalId( id, session );
	}

	@Override
	public CompletionStage<Object> reactiveResolveNaturalIdToId(
			Object naturalIdValue,
			SharedSessionContractImplementor session) {
		// TODO: Move it to the delegator
		return reactiveSelectByNaturalId(
				naturalIdMapping().normalizeInput( naturalIdValue ),
				NaturalIdLoadOptions.NONE,
				(tableGroup, creationState) -> entityDescriptor().getIdentifierMapping().createDomainResult(
						tableGroup.getNavigablePath().append( EntityIdentifierMapping.ROLE_LOCAL_NAME ),
						tableGroup,
						null,
						creationState
				),
				ReactiveSimpleNaturalIdLoader::visitFetches,
				(statsEnabled) -> {
//					entityDescriptor().getPreLoadListener().startingLoad( entityDescriptor, naturalIdValue, KeyType.NATURAL_ID, LoadSource.DATABASE );
					return statsEnabled ? System.nanoTime() : -1L;
				},
				(result, startToken) -> {
//					entityDescriptor().getPostLoadListener().completedLoad( result, entityDescriptor(), naturalIdValue, KeyType.NATURAL_ID, LoadSource.DATABASE );
					if ( startToken > 0 ) {
						session.getFactory().getStatistics().naturalIdQueryExecuted(
								entityDescriptor().getEntityPersister().getRootEntityName(),
								System.nanoTime() - startToken
						);
//						// todo (6.0) : need a "load-by-natural-id" stat
//						//		e.g.,
//						// final Object identifier = entityDescriptor().getIdentifierMapping().getIdentifier( result, session );
//						// session.getFactory().getStatistics().entityLoadedByNaturalId( entityDescriptor(), identifier );
					}
				},
				session
		);
	}

	@Override
	public CompletionStage<T> load(
			Object naturalIdValue,
			NaturalIdLoadOptions options,
			SharedSessionContractImplementor session) {
		// TODO: Move it to the delegator
		return reactiveSelectByNaturalId(
				naturalIdMapping().normalizeInput( naturalIdValue ),
				options,
				(tableGroup, creationState) -> entityDescriptor().createDomainResult(
						new NavigablePath( entityDescriptor().getRootPathName() ),
						tableGroup,
						null,
						creationState
				),
				ReactiveSimpleNaturalIdLoader::visitFetches,
				(statsEnabled) -> {
//					entityDescriptor().getPreLoadListener().startingLoad( entityDescriptor, naturalIdValue, KeyType.NATURAL_ID, LoadSource.DATABASE );
					return statsEnabled ? System.nanoTime() : -1;
				},
				(result, startToken) -> {
//					entityDescriptor().getPostLoadListener().completedLoad( result, entityDescriptor(), naturalIdValue, KeyType.NATURAL_ID, LoadSource.DATABASE );
					if ( startToken > 0 ) {
						session.getFactory().getStatistics().naturalIdQueryExecuted(
								entityDescriptor().getEntityPersister().getRootEntityName(),
								System.nanoTime() - startToken
						);
//						// todo (6.0) : need a "load-by-natural-id" stat
//						//		e.g.,
//						// final Object identifier = entityDescriptor().getIdentifierMapping().getIdentifier( result, session );
//						// session.getFactory().getStatistics().entityLoadedByNaturalId( entityDescriptor(), identifier );
					}
				},
				session
		).thenApply( this::castToClassType );
	}

	// TODO: Move it to the delegator
	private static ImmutableFetchList visitFetches(
			FetchParent fetchParent,
			LoaderSqlAstCreationState creationState) {
		final FetchableContainer fetchableContainer = fetchParent.getReferencedMappingContainer();
		final int size = fetchableContainer.getNumberOfFetchables();
		final ImmutableFetchList.Builder fetches = new ImmutableFetchList.Builder( fetchableContainer );
		for ( int i = 0; i < size; i++ ) {
			final Fetchable fetchable = fetchableContainer.getFetchable( i );
			final NavigablePath navigablePath = fetchParent.resolveNavigablePath( fetchable );
			final Fetch fetch = fetchParent.generateFetchableFetch(
					fetchable,
					navigablePath,
					fetchable.getMappedFetchOptions().getTiming(),
					true,
					null,
					creationState
			);
			fetches.add( fetch );
		}
		return fetches.build();
	}

	// TODO: Move it to the delegator
	protected CompletionStage<Object> reactiveSelectByNaturalId(
			Object bindValue,
			NaturalIdLoadOptions options,
			BiFunction<TableGroup, LoaderSqlAstCreationState, DomainResult<?>> domainResultProducer,
			LoaderSqlAstCreationState.FetchProcessor fetchProcessor,
			Function<Boolean, Long> statementStartHandler,
			BiConsumer<Object, Long> statementCompletionHandler,
			SharedSessionContractImplementor session) {
		final SessionFactoryImplementor sessionFactory = session.getFactory();
		final JdbcServices jdbcServices = sessionFactory.getJdbcServices();
		final JdbcEnvironment jdbcEnvironment = jdbcServices.getJdbcEnvironment();
		final SqlAstTranslatorFactory sqlAstTranslatorFactory = jdbcEnvironment.getSqlAstTranslatorFactory();

		final LockOptions lockOptions;
		if ( options.getLockOptions() != null ) {
			lockOptions = options.getLockOptions();
		}
		else {
			lockOptions = LockOptions.NONE;
		}

		final NavigablePath entityPath = new NavigablePath( entityDescriptor().getRootPathName() );
		final QuerySpec rootQuerySpec = new QuerySpec( true );

		final LoaderSqlAstCreationState sqlAstCreationState = new LoaderSqlAstCreationState(
				rootQuerySpec,
				new SqlAliasBaseManager(),
				new SimpleFromClauseAccessImpl(),
				lockOptions,
				fetchProcessor,
				true,
				sessionFactory
		);

		final TableGroup rootTableGroup = entityDescriptor().createRootTableGroup(
				true,
				entityPath,
				null,
				() -> rootQuerySpec::applyPredicate,
				sqlAstCreationState,
				sessionFactory
		);

		rootQuerySpec.getFromClause().addRoot( rootTableGroup );
		sqlAstCreationState.getFromClauseAccess().registerTableGroup( entityPath, rootTableGroup );

		final DomainResult<?> domainResult = domainResultProducer.apply( rootTableGroup, sqlAstCreationState );

		final SelectStatement sqlSelect = new SelectStatement(
				rootQuerySpec,
				Collections.singletonList( domainResult )
		);

		final JdbcParameterBindings jdbcParamBindings = new JdbcParameterBindingsImpl( naturalIdMapping().getJdbcTypeCount() );

		applyNaturalIdRestriction(
				bindValue,
				rootTableGroup,
				rootQuerySpec::applyPredicate,
				jdbcParamBindings::addBinding,
				sqlAstCreationState,
				session
		);

		final QueryOptions queryOptions = new SimpleQueryOptions( lockOptions, false );
		final JdbcOperationQuerySelect jdbcSelect = sqlAstTranslatorFactory.buildSelectTranslator(
						sessionFactory,
						sqlSelect
				)
				.translate( jdbcParamBindings, queryOptions );

		final StatisticsImplementor statistics = sessionFactory.getStatistics();
		final Long startToken = statementStartHandler.apply( statistics.isStatisticsEnabled() );

		return StandardReactiveSelectExecutor.INSTANCE
				.list(
						jdbcSelect,
						jdbcParamBindings,
						new NaturalIdLoaderWithOptionsExecutionContext( session, queryOptions ),
						row -> row[0],
						ReactiveListResultsConsumer.UniqueSemantic.FILTER
				)
				.thenApply( results -> {
					if ( results.size() > 1 ) {
						throw new HibernateException(
								String.format(
										"Loading by natural-id returned more that one row : %s",
										getLoadable().getEntityName()
								)
						);
					}

					final T result = results.isEmpty()
							? null
							: (T) results.get( 0 );

					statementCompletionHandler.accept( result, startToken );
					return result;
				} );
	}

	private T castToClassType(Object o) {
		return (T) o;
	}

	private static class NaturalIdLoaderWithOptionsExecutionContext extends BaseExecutionContext {
		private final Callback callback;
		private final QueryOptions queryOptions;

		public NaturalIdLoaderWithOptionsExecutionContext(
				SharedSessionContractImplementor session,
				QueryOptions queryOptions) {
			super( session );
			this.queryOptions = queryOptions;
			callback = new CallbackImpl();
		}

		@Override
		public QueryOptions getQueryOptions() {
			return queryOptions;
		}

		@Override
		public Callback getCallback() {
			return callback;
		}

	}
}
