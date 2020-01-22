package org.hibernate.rx.loader.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
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
import org.hibernate.rx.impl.RxQueryExecutor;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class RxAbstractEntityLoader extends AbstractEntityLoader {

	public RxAbstractEntityLoader(OuterJoinLoadable persister, Type uniqueKeyType, SessionFactoryImplementor factory,
								  LoadQueryInfluencers loadQueryInfluencers) {
		super(persister, uniqueKeyType, factory, loadQueryInfluencers);
	}

	@Override
	protected CompletionStage<Optional<Object>> load(
			SharedSessionContractImplementor session,
			Object id,
			Object optionalObject,
			Serializable optionalId,
			LockOptions lockOptions,
			Boolean readOnly) {

		return loadRxEntity(
				(SessionImplementor) session,
				id,
				uniqueKeyType,
				optionalObject,
				entityName,
				optionalId,
				persister,
				lockOptions
		).thenApply( list -> {
			if ( list.size() == 1 ) {
				return Optional.ofNullable( list.get( 0 ) );
			}
			if ( list.size() == 0 ) {
				return Optional.empty();
			}
			if ( getCollectionOwners() != null ) {
				return Optional.ofNullable( list.get( 0 ) );
			}
			throw new HibernateException(
					"More than one row with the given identifier was found: " +
							id +
							", for class: " +
							persister.getEntityName()
			);
		} );
	}

	protected CompletionStage<List<?>> loadRxEntity(
			final SessionImplementor session,
			final Object id,
			final Type identifierType,
			final Object optionalObject,
			final String optionalEntityName,
			final Serializable optionalIdentifier,
			final EntityPersister persister,
			LockOptions lockOptions) throws HibernateException {

		CompletionStage<List<?>> result;
		try {
			QueryParameters qp = new QueryParameters();
			qp.setPositionalParameterTypes( new Type[] { identifierType } );
			qp.setPositionalParameterValues( new Object[] { id } );
			qp.setOptionalObject( optionalObject );
			qp.setOptionalEntityName( optionalEntityName );
			qp.setOptionalId( optionalIdentifier );
			qp.setLockOptions( lockOptions );

			result = doRxQueryAndInitializeNonLazyCollections( session, qp, false );

			LOG.debug( "Done entity load" );
			return result;
		}
		catch (SQLException sqle) {
			final Loadable[] persisters = getEntityPersisters();
			throw this.getFactory().getJdbcServices().getSqlExceptionHelper().convert(
					sqle,
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
	}

	protected CompletionStage<List<?>> doRxQueryAndInitializeNonLazyCollections(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies)
			throws HibernateException, SQLException {
		return doRxQueryAndInitializeNonLazyCollections( session, queryParameters, returnProxies, null );
	}

	protected CompletionStage<List<?>> doRxQueryAndInitializeNonLazyCollections(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer)
			throws HibernateException, SQLException {
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
		try {
			CompletionStage<List<?>>  result;
			try {
				result = doRxQuery( session, queryParameters, returnProxies, forcedResultTransformer );
			}
			finally {
				persistenceContext.afterLoad();
			}
			persistenceContext.initializeNonLazyCollections();
			return result;
		}
		finally {
			// Restore the original default
			persistenceContext.setDefaultReadOnly( defaultReadOnlyOrig );
		}
	}

	private CompletionStage<List<?>> doRxQuery(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer) throws HibernateException {

		final RowSelection selection = queryParameters.getRowSelection();
		final int maxRows = LimitHelper.hasMaxRows( selection ) ?
				selection.getMaxRows() :
				Integer.MAX_VALUE;

		final List<AfterLoadAction> afterLoadActions = new ArrayList<AfterLoadAction>();

		return executeRxQueryStatement(
				getSQLString(), queryParameters, false, afterLoadActions, session,
				resultSet -> {
					try {
						return processResultSet(
								resultSet,
								queryParameters,
								session,
								returnProxies,
								forcedResultTransformer,
								maxRows,
								afterLoadActions
						);
					}
					catch (SQLException ex) {
						//never actually happens
						throw new JDBCException( "error querying", ex );
					}
				}
		);
	}

	protected CompletionStage<List<?>> executeRxQueryStatement(
			String sqlStatement,
			QueryParameters queryParameters,
			boolean scroll,
			List<AfterLoadAction> afterLoadActions,
			SessionImplementor session,
			Function<ResultSet, List<Object>> transformer) {

		// Processing query filters.
		queryParameters.processFilters( sqlStatement, session );

		// Applying LIMIT clause.
		final LimitHandler limitHandler = getLimitHandler(
				queryParameters.getRowSelection()
		);
		String sql = limitHandler.processSql( queryParameters.getFilteredSQL(), queryParameters.getRowSelection() );

		// Adding locks and comments.
		sql = preprocessSQL( sql, queryParameters, getFactory(), afterLoadActions );

		return new RxQueryExecutor().execute( sql, queryParameters, session, transformer );
	}
}
