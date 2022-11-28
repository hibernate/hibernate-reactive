/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.BatchFetchQueue;
import org.hibernate.engine.spi.CollectionKey;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.internal.util.collections.CollectionHelper;
import org.hibernate.loader.ast.internal.CollectionLoaderSubSelectFetch;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.QueryParameterBindings;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.exec.spi.Callback;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcSelect;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.entity.LoadingEntityEntry;
import org.hibernate.sql.results.internal.ResultsHelper;
import org.hibernate.sql.results.internal.RowTransformerStandardImpl;

public class ReactiveCollectionLoaderSubSelectFetch extends CollectionLoaderSubSelectFetch implements ReactiveCollectionLoader {

	private PluralAttributeMapping attributeMapping;
	private SubselectFetch subselect;

	public ReactiveCollectionLoaderSubSelectFetch(PluralAttributeMapping attributeMapping, DomainResult cachedDomainResult, SubselectFetch subselect, SharedSessionContractImplementor session) {
		super( attributeMapping, cachedDomainResult, subselect, session );
		this.attributeMapping = attributeMapping;
		this.subselect = subselect;
	}

	@Override
	public CompletionStage<PersistentCollection<?>> reactiveLoad(Object triggerKey, SharedSessionContractImplementor session) {
		final CollectionKey collectionKey = new CollectionKey( attributeMapping.getCollectionDescriptor(), triggerKey );

		final SessionFactoryImplementor sessionFactory = session.getFactory();
		final JdbcServices jdbcServices = sessionFactory.getJdbcServices();
		final JdbcEnvironment jdbcEnvironment = jdbcServices.getJdbcEnvironment();
		final SqlAstTranslatorFactory sqlAstTranslatorFactory = jdbcEnvironment.getSqlAstTranslatorFactory();
		final PersistenceContext persistenceContext = session.getPersistenceContext();

		// try to find a registered SubselectFetch
		final PersistentCollection<?> collection = persistenceContext.getCollection( collectionKey );
		attributeMapping.getCollectionDescriptor().getCollectionType().getKeyOfOwner( collection.getOwner(), session );

		final BatchFetchQueue batchFetchQueue = persistenceContext.getBatchFetchQueue();
		final EntityEntry ownerEntry = persistenceContext.getEntry( collection.getOwner() );
		List<PersistentCollection<?>> subSelectFetchedCollections = null;
		if ( ownerEntry != null ) {
			final EntityKey triggerKeyOwnerKey = ownerEntry.getEntityKey();
			final SubselectFetch registeredFetch = batchFetchQueue.getSubselect( triggerKeyOwnerKey );
			if ( registeredFetch != null ) {
				subSelectFetchedCollections = CollectionHelper
						.arrayList( registeredFetch.getResultingEntityKeys().size() );

				// there was one, so we want to make sure to prepare the corresponding collection
				// reference for reading
				final Iterator<EntityKey> itr = registeredFetch.getResultingEntityKeys().iterator();
				while ( itr.hasNext() ) {
					final EntityKey key = itr.next();

					final PersistentCollection<?> containedCollection = persistenceContext
							.getCollection( new CollectionKey( attributeMapping.getCollectionDescriptor(), key.getIdentifier() ) );

					if ( containedCollection != collection ) {
						containedCollection.beginRead();
						containedCollection.beforeInitialize( getLoadable().getCollectionDescriptor(), -1 );

						subSelectFetchedCollections.add( containedCollection );
					}
				}
			}
		}

		final JdbcSelect jdbcSelect = sqlAstTranslatorFactory
				.buildSelectTranslator( sessionFactory, getSqlAst() )
				.translate( this.subselect.getLoadingJdbcParameterBindings(), QueryOptions.NONE );

		final SubselectFetch.RegistrationHandler subSelectFetchableKeysHandler = SubselectFetch
				.createRegistrationHandler(
				batchFetchQueue,
				getSqlAst(),
				this.subselect.getLoadingJdbcParameters(),
				this.subselect.getLoadingJdbcParameterBindings()
		);

		final List<PersistentCollection<?>> fetchedCollections = subSelectFetchedCollections;
		return StandardReactiveSelectExecutor.INSTANCE
				.list( jdbcSelect, this.subselect.getLoadingJdbcParameterBindings(), executionContext( session, subSelectFetchableKeysHandler ), RowTransformerStandardImpl.instance(), ReactiveListResultsConsumer.UniqueSemantic.FILTER )
				.thenApply( rs -> {
					if ( fetchedCollections != null && !fetchedCollections.isEmpty() ) {
						fetchedCollections.forEach( c -> initializeSubCollection( persistenceContext, c ) );
						fetchedCollections.clear();
					}
					return collection;
				} );
	}

	private void initializeSubCollection(PersistenceContext persistenceContext, PersistentCollection<?> c) {
		if ( !c.wasInitialized() ) {
			c.initializeEmptyCollection( getLoadable().getCollectionDescriptor() );
			ResultsHelper.finalizeCollectionLoading( persistenceContext, getLoadable().getCollectionDescriptor(), c, c.getKey(), true );
		}
	}

	private static ExecutionContext executionContext(
			SharedSessionContractImplementor session,
			SubselectFetch.RegistrationHandler subSelectFetchableKeysHandler) {
		return new ExecutionContext() {
			@Override
			public SharedSessionContractImplementor getSession() {
				return session;
			}

			@Override
			public QueryOptions getQueryOptions() {
				return QueryOptions.NONE;
			}

			@Override
			public String getQueryIdentifier(String sql) {
				return sql;
			}

			@Override
			public void registerLoadingEntityEntry(EntityKey entityKey, LoadingEntityEntry entry) {
				subSelectFetchableKeysHandler.addKey( entityKey, entry );
			}

			@Override
			public QueryParameterBindings getQueryParameterBindings() {
				return QueryParameterBindings.NO_PARAM_BINDINGS;
			}

			@Override
			public Callback getCallback() {
				return null;
			}

		};
	}
}
