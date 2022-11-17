/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockOptions;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.CollectionKey;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.loader.ast.internal.CollectionLoaderBatchKey;
import org.hibernate.loader.ast.internal.LoaderSelectBuilder;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.QueryParameterBindings;
import org.hibernate.reactive.sql.exec.internal.ReactiveSelectExecutorStandardImpl;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.ast.Clause;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.Callback;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcSelect;
import org.hibernate.sql.results.graph.entity.LoadingEntityEntry;
import org.hibernate.sql.results.internal.RowTransformerStandardImpl;

import org.jboss.logging.Logger;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * @see org.hibernate.loader.ast.internal.CollectionLoaderBatchKey
 */
public class ReactiveCollectionLoaderBatchKey implements ReactiveCollectionLoader {

	private static final Logger log = Logger.getLogger( CollectionLoaderBatchKey.class );

	private final PluralAttributeMapping attributeMapping;
	private final int batchSize;

	private final int keyJdbcCount;

	private SelectStatement batchSizeSqlAst;
	private List<JdbcParameter> batchSizeJdbcParameters;

	public ReactiveCollectionLoaderBatchKey(
			PluralAttributeMapping attributeMapping,
			int batchSize,
			LoadQueryInfluencers influencers,
			SessionFactoryImplementor sessionFactory) {
		this.attributeMapping = attributeMapping;
		this.batchSize = batchSize;

		this.keyJdbcCount = attributeMapping.getKeyDescriptor().getJdbcTypeCount();

		this.batchSizeJdbcParameters = new ArrayList<>();
		this.batchSizeSqlAst = LoaderSelectBuilder.createSelect(
				attributeMapping,
				null,
				attributeMapping.getKeyDescriptor(),
				null,
				batchSize,
				influencers,
				LockOptions.NONE,
				batchSizeJdbcParameters::add,
				sessionFactory
		);
	}

	@Override
	public PluralAttributeMapping getLoadable() {
		return attributeMapping;
	}

	@Override
	public CompletionStage<PersistentCollection<?>> reactiveLoad(Object key, SharedSessionContractImplementor session) {
		final Object[] batchIds = session.getPersistenceContextInternal()
				.getBatchFetchQueue()
				.getCollectionBatch( getLoadable().getCollectionDescriptor(), key, batchSize );

		final int numberOfIds = ArrayHelper.countNonNull( batchIds );

		if ( numberOfIds == 1 ) {
			final List<JdbcParameter> jdbcParameters = new ArrayList<>( keyJdbcCount );
			final SelectStatement sqlAst = LoaderSelectBuilder.createSelect(
					attributeMapping,
					null,
					attributeMapping.getKeyDescriptor(),
					null,
					batchSize,
					session.getLoadQueryInfluencers(),
					LockOptions.NONE,
					jdbcParameters::add,
					session.getFactory()
			);

			return new ReactiveSingleIdLoadPlan<>(
					null,
					attributeMapping.getKeyDescriptor(),
					sqlAst,
					jdbcParameters,
					LockOptions.NONE,
					session.getFactory()
			)
			.load( key, session )
			.thenApply( obj -> {
				final CollectionKey collectionKey = new CollectionKey( attributeMapping.getCollectionDescriptor(), key );
				return session.getPersistenceContext().getCollection( collectionKey );
			} );
		}
		else {
			return batchLoad( batchIds, numberOfIds , session )
					.thenApply( v -> {
						final CollectionKey collectionKey = new CollectionKey( attributeMapping.getCollectionDescriptor(), key );
						return session.getPersistenceContext().getCollection( collectionKey );
					} );
		}
	}

	private CompletionStage<Void> batchLoad(
			Object[] batchIds,
			int numberOfIds,
			SharedSessionContractImplementor session) {
		if ( log.isDebugEnabled() ) {
			log.debugf( "Batch loading collection [%s] : %s", getLoadable().getCollectionDescriptor().getRole(), batchIds );
		}

		return whileLoop( batchIds, numberOfIds, session, 0, 0, voidFuture() );
	}

	// FIXME: Review this later: should I use the trampoline?
	private CompletionStage<Void> whileLoop(
			Object[] batchIds,
			int numberOfIds,
			SharedSessionContractImplementor session,
			int smallBatchStart,
			int smallBatchLength,
			CompletionStage<Void> loop) {
		if ( smallBatchStart < numberOfIds ) {
			final int newBatchLength = Math.min( numberOfIds - smallBatchLength, batchSize );
			return loop.thenCompose( v -> doWhile(
					batchIds,
					numberOfIds,
					session,
					smallBatchStart + newBatchLength,
					newBatchLength
			) );
		}
		return loop;
	}

	private CompletionStage<Void> doWhile(
			Object[] batchIds,
			int numberOfIds,
			SharedSessionContractImplementor session,
			int smallBatchStart,
			int smallBatchLength) {
		final List<JdbcParameter> jdbcParameters;
		final SelectStatement sqlAst;

		if ( smallBatchLength == batchSize ) {
			jdbcParameters = this.batchSizeJdbcParameters;
			sqlAst = this.batchSizeSqlAst;
		}
		else {
			jdbcParameters = new ArrayList<>();
			sqlAst = LoaderSelectBuilder.createSelect(
					getLoadable(),
					// null here means to select everything
					null,
					getLoadable().getKeyDescriptor(),
					null,
					numberOfIds,
					session.getLoadQueryInfluencers(),
					LockOptions.NONE,
					jdbcParameters::add,
					session.getFactory()
			);
		}

		final SessionFactoryImplementor sessionFactory = session.getFactory();
		final JdbcServices jdbcServices = sessionFactory.getJdbcServices();
		final JdbcEnvironment jdbcEnvironment = jdbcServices.getJdbcEnvironment();
		final SqlAstTranslatorFactory sqlAstTranslatorFactory = jdbcEnvironment.getSqlAstTranslatorFactory();

		final JdbcSelect jdbcSelect = sqlAstTranslatorFactory
				.buildSelectTranslator( sessionFactory, sqlAst )
				.translate( null, QueryOptions.NONE );

		final JdbcParameterBindings jdbcParameterBindings = new JdbcParameterBindingsImpl( keyJdbcCount * smallBatchLength );
		jdbcSelect.bindFilterJdbcParameters( jdbcParameterBindings );

		int offset = 0;

		for ( int i = smallBatchStart; i < smallBatchStart + smallBatchLength; i++ ) {
			offset += jdbcParameterBindings.registerParametersForEachJdbcValue(
					batchIds[i],
					Clause.WHERE,
					offset,
					getLoadable().getKeyDescriptor(),
					jdbcParameters,
					session
			);
		}
		assert offset == jdbcParameters.size();

		final SubselectFetch.RegistrationHandler subSelectFetchableKeysHandler = SubselectFetch
				.createRegistrationHandler(
						session.getPersistenceContext().getBatchFetchQueue(),
						sqlAst,
						Collections.emptyList(),
						jdbcParameterBindings
				);

		return new ReactiveSelectExecutorStandardImpl()
				.list(
						jdbcSelect,
						jdbcParameterBindings,
						executionContext( session, subSelectFetchableKeysHandler ),
						RowTransformerStandardImpl.instance(),
						ReactiveListResultsConsumer.UniqueSemantic.FILTER
				)
				.thenCompose( CompletionStages::voidFuture );
	}

	private static ExecutionContext executionContext(
			SharedSessionContractImplementor session,
			SubselectFetch.RegistrationHandler subSelectFetchableKeysHandler) {
		ExecutionContext executionContext = new ExecutionContext() {
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
		return executionContext;
	}
}
