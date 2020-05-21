/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.loader.JoinWalker;
import org.hibernate.loader.collection.BasicCollectionJoinWalker;
import org.hibernate.loader.collection.OneToManyJoinWalker;
import org.hibernate.persister.collection.QueryableCollection;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;


/**
 * A BatchingCollectionInitializerBuilder that builds CollectionInitializer instances capable of dynamically building
 * its batch-fetch SQL based on the actual number of collections keys waiting to be fetched.
 *
 * @author Gavin King
 */
public class ReactiveDynamicBatchingCollectionInitializerBuilder extends ReactiveBatchingCollectionInitializerBuilder {
	public static final ReactiveDynamicBatchingCollectionInitializerBuilder INSTANCE = new ReactiveDynamicBatchingCollectionInitializerBuilder();

	@Override
	protected ReactiveCollectionLoader createRealBatchingCollectionInitializer(
			QueryableCollection persister,
			int maxBatchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new ReactiveDynamicBatchingCollectionInitializer( persister, maxBatchSize, factory, influencers );
	}

	@Override
	protected ReactiveCollectionLoader createRealBatchingOneToManyInitializer(
			QueryableCollection persister,
			int maxBatchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new ReactiveDynamicBatchingCollectionInitializer( persister, maxBatchSize, factory, influencers );
	}

	public static class ReactiveDynamicBatchingCollectionInitializer extends ReactiveCollectionLoader {
		private final int maxBatchSize;
		private final ReactiveCollectionLoader singleKeyLoader;
		private final ReactiveDynamicBatchingCollectionLoader batchLoader;

		public ReactiveDynamicBatchingCollectionInitializer(
				QueryableCollection collectionPersister,
				int maxBatchSize,
				SessionFactoryImplementor factory,
				LoadQueryInfluencers influencers) {
			super( collectionPersister, factory, influencers );
			this.maxBatchSize = maxBatchSize;

			if ( collectionPersister.isOneToMany() ) {
				this.singleKeyLoader = new ReactiveOneToManyLoader( collectionPersister, 1, factory, influencers );
			}
			else {
				throw new UnsupportedOperationException();
//				this.singleKeyLoader = new ReactiveBasicCollectionLoader( collectionPersister, 1, factory, influencers );
			}

			this.batchLoader = new ReactiveDynamicBatchingCollectionLoader( collectionPersister, factory, influencers );
		}

		@Override
		public void initialize(Serializable id, SharedSessionContractImplementor session) throws HibernateException {
			// first, figure out how many batchable ids we have...
			final Serializable[] batch = session.getPersistenceContextInternal()
					.getBatchFetchQueue()
					.getCollectionBatch( collectionPersister(), id, maxBatchSize );
			final int numberOfIds = ArrayHelper.countNonNull( batch );
			if ( numberOfIds <= 1 ) {
				singleKeyLoader.loadCollection( session, id, collectionPersister().getKeyType() );
				return;
			}

			final Serializable[] idsToLoad = new Serializable[numberOfIds];
			System.arraycopy( batch, 0, idsToLoad, 0, numberOfIds );

			batchLoader.doBatchedCollectionLoad( (SessionImplementor) session, idsToLoad, collectionPersister().getKeyType() );
		}

		@Override
		public CompletionStage<Void> reactiveInitialize(Serializable id, SharedSessionContractImplementor session) {
			final Serializable[] batch = session.getPersistenceContextInternal()
					.getBatchFetchQueue()
					.getCollectionBatch( collectionPersister(), id, maxBatchSize );
			final int numberOfIds = ArrayHelper.countNonNull( batch );
			if ( numberOfIds <= 1 ) {
				return singleKeyLoader.reactiveLoadCollection( (SessionImplementor) session, id,
						collectionPersister().getKeyType() );
			}

			final Serializable[] idsToLoad = new Serializable[numberOfIds];
			System.arraycopy( batch, 0, idsToLoad, 0, numberOfIds );

			return batchLoader.doBatchedCollectionLoad( (SessionImplementor) session, idsToLoad,
					collectionPersister().getKeyType() );
		}
	}

	private static class ReactiveDynamicBatchingCollectionLoader extends ReactiveCollectionLoader {

		private final String sqlTemplate;
		private final String alias;

		public ReactiveDynamicBatchingCollectionLoader(
				QueryableCollection collectionPersister,
				SessionFactoryImplementor factory,
				LoadQueryInfluencers influencers) {
			super( collectionPersister, factory, influencers );

			JoinWalker walker = buildJoinWalker( collectionPersister, factory, influencers );
			initFromWalker( walker );
			this.sqlTemplate = walker.getSQLString();
			this.alias = StringHelper.generateAlias( collectionPersister.getRole(), 0 );
			postInstantiate();

			if ( LOG.isDebugEnabled() ) {
				LOG.debugf(
						"SQL-template for dynamic collection [%s] batch-fetching : %s",
						collectionPersister.getRole(),
						sqlTemplate
				);
			}
		}

		private JoinWalker buildJoinWalker(
				QueryableCollection collectionPersister,
				SessionFactoryImplementor factory,
				LoadQueryInfluencers influencers) {

			if ( collectionPersister.isOneToMany() ) {
				return new OneToManyJoinWalker( collectionPersister, -1, null, factory, influencers ) {
					@Override
					protected StringBuilder whereString(String alias, String[] columnNames, String subselect, int batchSize) {
						if ( subselect != null ) {
							return super.whereString( alias, columnNames, subselect, batchSize );
						}

						return StringHelper.buildBatchFetchRestrictionFragment( alias, columnNames, getFactory().getDialect() );
					}
				};
			}
			else {
				return new BasicCollectionJoinWalker( collectionPersister, -1, null, factory, influencers ) {
					@Override
					protected StringBuilder whereString(String alias, String[] columnNames, String subselect, int batchSize) {
						if ( subselect != null ) {
							return super.whereString( alias, columnNames, subselect, batchSize );
						}

						return StringHelper.buildBatchFetchRestrictionFragment( alias, columnNames, getFactory().getDialect() );
					}
				};
			}
		}

		public final CompletionStage<Void> doBatchedCollectionLoad(
				final SessionImplementor session,
				final Serializable[] ids,
				final Type type) throws HibernateException {

			if ( LOG.isDebugEnabled() ) {
				LOG.debugf(
						"Batch loading collection: %s",
						MessageHelper.collectionInfoString( getCollectionPersisters()[0], ids, getFactory() )
				);
			}

			final Type[] idTypes = new Type[ids.length];
			Arrays.fill( idTypes, type );
			final QueryParameters queryParameters = new QueryParameters( idTypes, ids, ids );

			final String sql = StringHelper.expandBatchIdPlaceholder(
					sqlTemplate,
					ids,
					alias,
					collectionPersister().getKeyColumnNames(),
					session.getJdbcServices().getJdbcEnvironment().getDialect()
			);

			return doReactiveQueryAndInitializeNonLazyCollections( sql, session, queryParameters, false, null )
					.handle( (list, e) -> {
						if (e instanceof JDBCException) {
							throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
									((JDBCException) e).getSQLException(),
									"could not initialize a collection batch: " +
											MessageHelper.collectionInfoString( getCollectionPersisters()[0], ids, getFactory() ),
									getSQLString()
							);
						} else if (e != null) {
							CompletionStages.rethrow(e);
						}
						LOG.debug("Done batch load");
						return null;
					} )
					.thenApply( list -> null );

		}

	}

}
