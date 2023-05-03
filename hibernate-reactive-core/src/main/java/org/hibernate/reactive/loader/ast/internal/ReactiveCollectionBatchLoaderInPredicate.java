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
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.BatchFetchQueue;
import org.hibernate.engine.spi.CollectionKey;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.internal.util.MutableInteger;
import org.hibernate.internal.util.collections.CollectionHelper;
import org.hibernate.loader.ast.internal.LoaderSelectBuilder;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

import static org.hibernate.loader.ast.internal.MultiKeyLoadLogging.MULTI_KEY_LOAD_DEBUG_ENABLED;
import static org.hibernate.loader.ast.internal.MultiKeyLoadLogging.MULTI_KEY_LOAD_LOGGER;

/**
 * @see org.hibernate.loader.ast.internal.CollectionBatchLoaderInPredicate
 */
public class ReactiveCollectionBatchLoaderInPredicate extends ReactiveAbstractCollectionBatchLoader {

	private final int keyColumnCount;
	private final int sqlBatchSize;
	private final List<JdbcParameter> jdbcParameters;
	private final SelectStatement sqlAst;
	private final JdbcOperationQuerySelect jdbcSelect;

	private ReactiveCollectionLoaderSingleKey singleKeyLoader;

	public ReactiveCollectionBatchLoaderInPredicate(
			int domainBatchSize,
			LoadQueryInfluencers influencers,
			PluralAttributeMapping attributeMapping,
			SessionFactoryImplementor sessionFactory) {
		super( domainBatchSize, influencers, attributeMapping, sessionFactory );

		this.keyColumnCount = attributeMapping.getKeyDescriptor().getJdbcTypeCount();
		this.sqlBatchSize = sessionFactory.getJdbcServices()
				.getDialect()
				.getBatchLoadSizingStrategy()
				.determineOptimalBatchLoadSize( keyColumnCount, domainBatchSize, false );
		if ( MULTI_KEY_LOAD_DEBUG_ENABLED ) {
			MULTI_KEY_LOAD_LOGGER.debugf(
					"Using IN-predicate batch fetching strategy for collection `%s` : %s (%s)",
					attributeMapping.getNavigableRole().getFullPath(),
					sqlBatchSize,
					domainBatchSize
			);
		}

		this.jdbcParameters = new ArrayList<>();
		this.sqlAst = LoaderSelectBuilder.createSelect(
				attributeMapping,
				null,
				attributeMapping.getKeyDescriptor(),
				null,
				sqlBatchSize,
				influencers,
				LockOptions.NONE,
				jdbcParameters::add,
				sessionFactory
		);
		assert this.jdbcParameters.size() == this.sqlBatchSize * this.keyColumnCount;

		this.jdbcSelect = sessionFactory.getJdbcServices()
				.getJdbcEnvironment()
				.getSqlAstTranslatorFactory()
				.buildSelectTranslator( sessionFactory, sqlAst )
				.translate( JdbcParameterBindings.NO_BINDINGS, QueryOptions.NONE );
	}

	private static void doNothing(Object key1, int relativePosition, int absolutePosition) {
	}

	@Override
	public CompletionStage<PersistentCollection<?>> reactiveLoad(Object key, SharedSessionContractImplementor session) {
		if ( MULTI_KEY_LOAD_DEBUG_ENABLED ) {
			MULTI_KEY_LOAD_LOGGER.debugf(
					"Loading collection `%s#%s` by batch-fetch",
					getLoadable().getNavigableRole().getFullPath(),
					key
			);
		}

		final MutableInteger nonNullCounter = new MutableInteger();
		final ArrayList<Object> keysToInitialize = CollectionHelper.arrayList( getDomainBatchSize() );
		session.getPersistenceContextInternal().getBatchFetchQueue().collectBatchLoadableCollectionKeys(
				getDomainBatchSize(),
				(index, batchableKey) -> {
					keysToInitialize.add( batchableKey );
					if ( batchableKey != null ) {
						nonNullCounter.increment();
					}
				},
				key,
				getLoadable().asPluralAttributeMapping()
		);

		if ( nonNullCounter.get() <= 0 ) {
			throw new IllegalStateException( "Number of non-null collection keys to batch fetch should never be 0" );
		}

		if ( nonNullCounter.get() == 1 ) {
			prepareSingleKeyLoaderIfNeeded();
			return singleKeyLoader.reactiveLoad( key, session );
		}

		return initializeKeys( key, keysToInitialize.toArray( keysToInitialize.toArray( new Object[0] ) ), nonNullCounter.get(), session )
				.thenApply( v -> {
					final CollectionKey collectionKey = new CollectionKey( getLoadable().getCollectionDescriptor(), key );
					return session.getPersistenceContext().getCollection( collectionKey );
				} );
	}

	private void prepareSingleKeyLoaderIfNeeded() {
		if ( singleKeyLoader == null ) {
			singleKeyLoader = new ReactiveCollectionLoaderSingleKey( getLoadable(), getInfluencers(), getSessionFactory() );
		}
	}

	private <T> CompletionStage<Void> initializeKeys(
			T key,
			T[] keysToInitialize,
			int nonNullKeysToInitializeCount,
			SharedSessionContractImplementor session) {
		if ( MULTI_KEY_LOAD_DEBUG_ENABLED ) {
			MULTI_KEY_LOAD_LOGGER.debugf(
					"Collection keys to batch-fetch initialize (`%s#%s`) %s",
					getLoadable().getNavigableRole().getFullPath(),
					key,
					keysToInitialize
			);
		}

		final ReactiveMultiKeyLoadChunker<T> chunker = new ReactiveMultiKeyLoadChunker<>(
				sqlBatchSize,
				keyColumnCount,
				getLoadable().getKeyDescriptor(),
				jdbcParameters,
				sqlAst,
				jdbcSelect
		);

		final BatchFetchQueue batchFetchQueue = session.getPersistenceContextInternal().getBatchFetchQueue();

		return chunker.processChunks(
				keysToInitialize,
				nonNullKeysToInitializeCount,
				(jdbcParameterBindings, session1) -> {
					// Create a RegistrationHandler for handling any subselect fetches we encounter handling this chunk
					final SubselectFetch.RegistrationHandler registrationHandler = SubselectFetch
							.createRegistrationHandler(
									batchFetchQueue,
									sqlAst,
									jdbcParameters,
									jdbcParameterBindings
							);
					return new ExecutionContextWithSubselectFetchHandler( session, registrationHandler );
				},
				ReactiveCollectionBatchLoaderInPredicate::doNothing,
				startIndex -> {
					if ( MULTI_KEY_LOAD_DEBUG_ENABLED ) {
						MULTI_KEY_LOAD_LOGGER.debugf(
								"Processing collection batch-fetch chunk (`%s#%s`) %s - %s",
								getLoadable().getNavigableRole().getFullPath(),
								key,
								startIndex,
								startIndex + ( sqlBatchSize - 1 )
						);
					}
				},
				(startIndex, nonNullElementCount) -> {
					if ( MULTI_KEY_LOAD_DEBUG_ENABLED ) {
						MULTI_KEY_LOAD_LOGGER.debugf(
								"Finishing collection batch-fetch chunk (`%s#%s`) %s - %s (%s)",
								getLoadable().getNavigableRole().getFullPath(),
								key,
								startIndex,
								startIndex + ( sqlBatchSize - 1 ),
								nonNullElementCount
						);
					}
					for ( int i = 0; i < nonNullElementCount; i++ ) {
						final int keyPosition = i + startIndex;
						if ( keyPosition < keysToInitialize.length ) {
							final T keyToInitialize = keysToInitialize[keyPosition];
							finishInitializingKey( keyToInitialize, session );
						}
					}
				},
				session
		);
	}
}
