package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.internal.util.StringHelper;
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

import static org.hibernate.internal.util.StringHelper.buildBatchFetchRestrictionFragment;

/**
 * A {@link ReactiveCollectionLoader} whose generated SQL contains a placeholder
 * that is interpolated with a batch of ids at runtime.
 *
 * Used when for {@link org.hibernate.loader.BatchFetchStyle#DYNAMIC} is selected.
 *
 * @see org.hibernate.loader.collection.DynamicBatchingCollectionInitializerBuilder.DynamicBatchingCollectionLoader
 */
class ReactiveDynamicBatchingCollectionInitializer extends ReactiveCollectionLoader {

	private final String sqlTemplate;
	private final String alias;

	public ReactiveDynamicBatchingCollectionInitializer(
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

					return buildBatchFetchRestrictionFragment( alias, columnNames, getFactory().getDialect() );
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

					return buildBatchFetchRestrictionFragment( alias, columnNames, getFactory().getDialect() );
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

		return doReactiveQueryAndInitializeNonLazyCollections( sql, session, queryParameters )
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
				} );

	}

}
