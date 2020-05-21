package org.hibernate.reactive.loader.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.LockOptions;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.engine.spi.*;
import org.hibernate.loader.entity.AbstractEntityLoader;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.Loadable;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.reactive.engine.impl.ReactivePersistenceContextAdapter;
import org.hibernate.reactive.impl.ReactiveSessionInternal;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static org.hibernate.reactive.adaptor.impl.QueryParametersAdaptor.toParameterArray;

public class ReactiveAbstractEntityLoader extends AbstractEntityLoader implements ReactiveUniqueEntityLoader {

	public ReactiveAbstractEntityLoader(OuterJoinLoadable persister, Type uniqueKeyType, SessionFactoryImplementor factory,
										LoadQueryInfluencers loadQueryInfluencers) {
		super(persister, uniqueKeyType, factory, loadQueryInfluencers);
	}

	@Override
	protected CompletionStage<Object> load(
			SharedSessionContractImplementor session,
			Object id,
			Object optionalObject,
			Serializable optionalId,
			LockOptions lockOptions,
			Boolean readOnly) {

		return loadReactiveEntity(
				(SessionImplementor) session,
				id,
				uniqueKeyType,
				optionalObject,
				entityName,
				optionalId,
				persister,
				lockOptions
		).thenApply( list -> {
			switch ( list.size() ) {
				case 1:
					return list.get( 0 );
				case 0:
					return null;
				default:
					if ( getCollectionOwners() != null ) {
						return list.get( 0 );
					}
			}
			throw new HibernateException(
					"More than one row with the given identifier was found: " +
							id +
							", for class: " +
							persister.getEntityName()
			);
		} );
	}

	protected CompletionStage<List<Object>> loadReactiveEntity(
			final SessionImplementor session,
			final Object id,
			final Type identifierType,
			final Object optionalObject,
			final String optionalEntityName,
			final Serializable optionalIdentifier,
			final EntityPersister persister,
			LockOptions lockOptions) throws HibernateException {

		QueryParameters qp = new QueryParameters();
		qp.setPositionalParameterTypes( new Type[] { identifierType } );
		qp.setPositionalParameterValues( new Object[] { id } );
		qp.setOptionalObject( optionalObject );
		qp.setOptionalEntityName( optionalEntityName );
		qp.setOptionalId( optionalIdentifier );
		qp.setLockOptions( lockOptions );

		return doReactiveQueryAndInitializeNonLazyCollections( session, qp, false )
			.handle( (list, e) -> {
				LOG.debug( "Done entity load" );
				if (e instanceof SQLException) {
					final Loadable[] persisters = getEntityPersisters();
					throw this.getFactory().getJdbcServices().getSqlExceptionHelper().convert(
							(SQLException) e,
							"could not load an entity: " +
									MessageHelper.infoString(
											persisters[persisters.length - 1],
											id,
											identifierType,
											getFactory()
									),
							getSQLString()
					);
				}
				else if (e !=null ) {
					CompletionStages.rethrow(e);
				}
				return list;
			});
	}

	protected CompletionStage<List<Object>> doReactiveQueryAndInitializeNonLazyCollections(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies) {
		return doReactiveQueryAndInitializeNonLazyCollections( getSQLString(), session, queryParameters, returnProxies, null );
	}

	protected CompletionStage<List<Object>> doReactiveQueryAndInitializeNonLazyCollections(
			final String sql,
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer) {
		final PersistenceContext persistenceContext = session.getPersistenceContext();
		boolean defaultReadOnlyOrig = persistenceContext.isDefaultReadOnly();
		if ( queryParameters.isReadOnlyInitialized() ) {
			// The read-only/modifiable mode for the query was explicitly set.
			// Temporarily set the default read-only/modifiable setting to the query's setting.
			persistenceContext.setDefaultReadOnly( queryParameters.isReadOnly() );
		}
		else {
			// The read-only/modifiable setting for the query was not initialized.
			// Use the default read-only/modifiable from the persistence context instead.
			queryParameters.setReadOnly( persistenceContext.isDefaultReadOnly() );
		}
		persistenceContext.beforeLoad();
		return doReactiveQuery( sql, session, queryParameters, returnProxies, forcedResultTransformer )
				.whenComplete( (list, e) -> persistenceContext.afterLoad() )
				.thenCompose( list ->
						// only initialize non-lazy collections after everything else has been refreshed
						((ReactivePersistenceContextAdapter) persistenceContext ).reactiveInitializeNonLazyCollections()
								.thenApply(v -> list)
				)
				.whenComplete( (list, e) -> persistenceContext.setDefaultReadOnly(defaultReadOnlyOrig) );
	}

	private CompletionStage<List<Object>> doReactiveQuery(
			String sql,
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer) throws HibernateException {

		final RowSelection selection = queryParameters.getRowSelection();
		final int maxRows = LimitHelper.hasMaxRows( selection ) ?
				selection.getMaxRows() :
				Integer.MAX_VALUE;

		final List<AfterLoadAction> afterLoadActions = new ArrayList<AfterLoadAction>();

		return executeReactiveQueryStatement(
				sql, queryParameters, false, afterLoadActions, session,
				resultSet -> {
					try {
						return processResultSet( resultSet, queryParameters, session, returnProxies,
								forcedResultTransformer, maxRows, afterLoadActions );
					}
					catch (SQLException sqle) {
						throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
								sqle,
								"could not load an entity batch: " + MessageHelper.infoString(
										getEntityPersisters()[0],
										queryParameters.getOptionalId(),
										session.getFactory()
								),
								sql
						);
					}
				}
		);
	}

	protected CompletionStage<List<Object>> executeReactiveQueryStatement(
			String sqlStatement,
			QueryParameters queryParameters,
			boolean scroll,
			List<AfterLoadAction> afterLoadActions,
			SessionImplementor session,
			Function<ResultSet, List<Object>> transformer) {

		// Processing query filters.
		queryParameters.processFilters( sqlStatement, session );

		// Applying LIMIT clause.
		final LimitHandler limitHandler = getLimitHandler( queryParameters.getRowSelection() );
		String sql = limitHandler.processSql( queryParameters.getFilteredSQL(), queryParameters.getRowSelection() );

		// Adding locks and comments.
		sql = preprocessSQL( sql, queryParameters, getFactory(), afterLoadActions );

		return ((ReactiveSessionInternal) session).getReactiveConnection()
				.selectJdbc( sql, toParameterArray(queryParameters, session) )
				.thenApply( transformer );
	}


	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session) {
		// this form is deprecated!
		return load( id, optionalObject, session, LockOptions.NONE, null );
	}

	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, Boolean readOnly) {
		// this form is deprecated!
		return load( id, optionalObject, session, LockOptions.NONE, readOnly );
	}

	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, LockOptions lockOptions) {
		return load( id, optionalObject, session, lockOptions, null );
	}

	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, LockOptions lockOptions, Boolean readOnly) {
		return load( session, id, optionalObject, id, lockOptions, readOnly );
	}
}
