/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockOptions;
import org.hibernate.engine.spi.BatchFetchQueue;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.loader.ast.internal.LoaderSelectBuilder;
import org.hibernate.loader.ast.internal.Preparable;
import org.hibernate.loader.ast.spi.EntityBatchLoader;
import org.hibernate.loader.ast.spi.SqlInPredicateMultiKeyLoader;
import org.hibernate.metamodel.mapping.EntityIdentifierMapping;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

import static org.hibernate.internal.util.collections.CollectionHelper.arrayList;
import static org.hibernate.loader.ast.internal.MultiKeyLoadLogging.MULTI_KEY_LOAD_DEBUG_ENABLED;
import static org.hibernate.loader.ast.internal.MultiKeyLoadLogging.MULTI_KEY_LOAD_LOGGER;

/**
 * @see org.hibernate.loader.ast.internal.EntityBatchLoaderInPredicate
 */
public class ReactiveEntityBatchLoaderInPredicate<T> extends ReactiveSingleIdEntityLoaderSupport<T>
		implements EntityBatchLoader<CompletionStage<T>>, SqlInPredicateMultiKeyLoader, Preparable {

	private final int domainBatchSize;
	private final int sqlBatchSize;

	private List<JdbcParameter> jdbcParameters;
	private SelectStatement sqlAst;
	private JdbcOperationQuerySelect jdbcSelectOperation;

	/**
	 * @param domainBatchSize The maximum number of entities we will initialize for each {@link #load load}
	 * @param sqlBatchSize The number of keys our SQL AST should be able to fetch
	 */
	public ReactiveEntityBatchLoaderInPredicate(
			int domainBatchSize,
			int sqlBatchSize,
			EntityMappingType entityDescriptor,
			SessionFactoryImplementor sessionFactory) {
		super( entityDescriptor, sessionFactory );
		this.domainBatchSize = domainBatchSize;
		this.sqlBatchSize = sqlBatchSize;

		if ( MULTI_KEY_LOAD_DEBUG_ENABLED ) {
			MULTI_KEY_LOAD_LOGGER.debugf(
					"Batch fetching `%s` entity using padded IN-list : %s (%s)",
					entityDescriptor.getEntityName(),
					domainBatchSize,
					sqlBatchSize
			);
		}
	}

	@Override
	public int getDomainBatchSize() {
		return domainBatchSize;
	}

	@Override
	public final CompletionStage<T> load(Object pkValue, LockOptions lockOptions, Boolean readOnly, SharedSessionContractImplementor session) {
		return load( pkValue, null, lockOptions, readOnly, session );
	}

	@Override
	public final CompletionStage<T> load(
			Object pkValue,
			Object entityInstance,
			LockOptions lockOptions,
			Boolean readOnly,
			SharedSessionContractImplementor session) {
		if ( MULTI_KEY_LOAD_DEBUG_ENABLED ) {
			MULTI_KEY_LOAD_LOGGER.debugf( "Batch loading entity `%s#%s`", getLoadable().getEntityName(), pkValue );
		}

		final Object[] idsToInitialize = resolveIdsToLoad( pkValue, session );
		if ( MULTI_KEY_LOAD_DEBUG_ENABLED ) {
			MULTI_KEY_LOAD_LOGGER.debugf(
					"Ids to batch-fetch initialize (`%s#%s`) %s",
					getLoadable().getEntityName(),
					pkValue,
					Arrays.toString( idsToInitialize )
			);
		}

		return initializeEntities( idsToInitialize, pkValue, entityInstance, lockOptions, readOnly, session )
				.thenApply( v -> {
					final EntityKey entityKey = session.generateEntityKey( pkValue, getLoadable().getEntityPersister() );
					//noinspection unchecked
					return (T) session.getPersistenceContext().getEntity( entityKey );
				} );
	}

	protected Object[] resolveIdsToLoad(Object pkValue, SharedSessionContractImplementor session) {
		return session.getPersistenceContextInternal().getBatchFetchQueue().getBatchLoadableEntityIds(
				getLoadable(),
				pkValue,
				domainBatchSize
		);
	}

	protected CompletionStage<Void> initializeEntities(
			Object[] idsToInitialize,
			Object pkValue,
			Object entityInstance,
			LockOptions lockOptions,
			Boolean readOnly,
			SharedSessionContractImplementor session) {
		final ReactiveMultiKeyLoadChunker<Object> chunker = new ReactiveMultiKeyLoadChunker<>(
				sqlBatchSize,
				getLoadable().getIdentifierMapping().getJdbcTypeCount(),
				getLoadable().getIdentifierMapping(),
				jdbcParameters,
				sqlAst,
				jdbcSelectOperation
		);

		final BatchFetchQueue batchFetchQueue = session.getPersistenceContextInternal().getBatchFetchQueue();
		final List<EntityKey> entityKeys = arrayList( sqlBatchSize );

		return chunker.processChunks(
				idsToInitialize,
				sqlBatchSize,
				(jdbcParameterBindings, session1) -> {
					// Create a RegistrationHandler for handling any subselect fetches we encounter handling this chunk
					final SubselectFetch.RegistrationHandler registrationHandler = SubselectFetch.createRegistrationHandler(
							batchFetchQueue,
							sqlAst,
							jdbcParameters,
							jdbcParameterBindings
					);
					return new SingleIdExecutionContext(
							pkValue,
							entityInstance,
							readOnly,
							lockOptions,
							registrationHandler,
							session
					);
				},
				(key, relativePosition, absolutePosition) -> {
					if ( key != null ) {
						entityKeys.add( session.generateEntityKey( key, getLoadable().getEntityPersister() ) );
					}
				},
				(startIndex) -> {
					if ( MULTI_KEY_LOAD_DEBUG_ENABLED ) {
						MULTI_KEY_LOAD_LOGGER.debugf(
								"Processing entity batch-fetch chunk (`%s#%s`) %s - %s",
								getLoadable().getEntityName(),
								pkValue,
								startIndex,
								startIndex + ( sqlBatchSize -1)
						);
					}
				},
				(startIndex, nonNullElementCount) -> {
					entityKeys.forEach( batchFetchQueue::removeBatchLoadableEntityKey );
					entityKeys.clear();
				},
				session
		);
	}

	@Override
	public void prepare() {
		EntityIdentifierMapping identifierMapping = getLoadable().getIdentifierMapping();

		final int expectedNumberOfParameters = identifierMapping.getJdbcTypeCount() * sqlBatchSize;

		jdbcParameters = arrayList( expectedNumberOfParameters );
		sqlAst = LoaderSelectBuilder.createSelect(
				getLoadable(),
				// null here means to select everything
				null,
				identifierMapping,
				null,
				sqlBatchSize,
				LoadQueryInfluencers.NONE,
				LockOptions.NONE,
				jdbcParameters::add,
				sessionFactory
		);
		assert jdbcParameters.size() == expectedNumberOfParameters;

		jdbcSelectOperation = sessionFactory.getJdbcServices()
				.getJdbcEnvironment()
				.getSqlAstTranslatorFactory()
				.buildSelectTranslator( sessionFactory, sqlAst )
				.translate( JdbcParameterBindings.NO_BINDINGS, QueryOptions.NONE );
	}

	@Override
	public String toString() {
		return String.format(
				Locale.ROOT,
				"EntityBatchLoaderInPredicate(%s [%s (%s)])",
				getLoadable().getEntityName(),
				domainBatchSize,
				sqlBatchSize
		);
	}
}
