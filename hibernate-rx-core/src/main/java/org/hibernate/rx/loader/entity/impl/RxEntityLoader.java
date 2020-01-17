package org.hibernate.rx.loader.entity.impl;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.entity.AbstractEntityLoader;
import org.hibernate.loader.entity.EntityJoinWalker;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.Loadable;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.rx.impl.RxQueryExecutor;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

/**
 * @see org.hibernate.loader.entity.EntityLoader
 */
public class RxEntityLoader extends AbstractEntityLoader implements UniqueEntityLoader {

	private final int[][] compositeKeyManyToOneTargetIndices;

	public RxEntityLoader(
			OuterJoinLoadable persister,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this( persister, 1, LockMode.NONE, factory, loadQueryInfluencers );
	}

	// We don't use all the parameters but I kept them for symmetry with EntityLoader
	public RxEntityLoader(
			OuterJoinLoadable persister,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this( persister, 1, lockMode, factory, loadQueryInfluencers );
	}

	public RxEntityLoader(
			OuterJoinLoadable persister,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this( persister, 1, lockOptions, factory, loadQueryInfluencers );
	}

	public RxEntityLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this(
				persister,
				persister.getIdentifierColumnNames(),
				persister.getIdentifierType(),
				batchSize,
				lockMode,
				factory,
				loadQueryInfluencers
		);
	}

	public RxEntityLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this(
				persister,
				persister.getIdentifierColumnNames(),
				persister.getIdentifierType(),
				batchSize,
				lockOptions,
				factory,
				loadQueryInfluencers
		);
	}

	public RxEntityLoader(
			OuterJoinLoadable persister,
			String[] uniqueKey,
			Type uniqueKeyType,
			int batchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this( persister, uniqueKey, uniqueKeyType, batchSize, lockMode, factory, loadQueryInfluencers,
				new PgEntityJoinWalker(
						persister,
						uniqueKey,
						batchSize,
						lockMode,
						factory,
						loadQueryInfluencers
				) );
	}

	public RxEntityLoader(
			OuterJoinLoadable persister,
			String[] uniqueKey,
			Type uniqueKeyType,
			int batchSize,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this( persister, uniqueKey, uniqueKeyType, batchSize, lockOptions.getLockMode(), factory, loadQueryInfluencers,
				new PgEntityJoinWalker(
						persister,
						uniqueKey,
						batchSize,
						lockOptions,
						factory,
						loadQueryInfluencers
				) );
	}

	private RxEntityLoader(
			OuterJoinLoadable persister,
			String[] uniqueKey,
			Type uniqueKeyType,
			int batchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers,
			EntityJoinWalker walker) throws MappingException {
		super( persister, uniqueKeyType, factory, loadQueryInfluencers );

		initFromWalker( walker );
		this.compositeKeyManyToOneTargetIndices = walker.getCompositeKeyManyToOneTargetIndices();
		postInstantiate();
	}

	@Override
	public int[][] getCompositeKeyManyToOneTargetIndices() {
		return compositeKeyManyToOneTargetIndices;
	}

	@Override
	public Object load(
			Serializable id, Object optionalObject, SharedSessionContractImplementor session)
			throws HibernateException {
		return load( id, optionalObject, session, LockOptions.NONE );
	}

	@Override
	public Object load(
			Serializable id,
			Object optionalObject,
			SharedSessionContractImplementor session,
			LockOptions lockOptions) {
		return load( session, id, optionalObject, id, lockOptions );
	}

	@Override
	protected Object load(
			SharedSessionContractImplementor session,
			Object id,
			Object optionalObject,
			Serializable optionalId,
			LockOptions lockOptions) {

		return loadRxEntity(
				session,
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

	protected CompletionStage<List> loadRxEntity(
			final SharedSessionContractImplementor session,
			final Object id,
			final Type identifierType,
			final Object optionalObject,
			final String optionalEntityName,
			final Serializable optionalIdentifier,
			final EntityPersister persister,
			LockOptions lockOptions) throws HibernateException {

		CompletionStage<List> result;
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

	public CompletionStage<List> doRxQueryAndInitializeNonLazyCollections(
			final SharedSessionContractImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies)
			throws HibernateException, SQLException {
		return doRxQueryAndInitializeNonLazyCollections( session, queryParameters, returnProxies, null );
	}

	public CompletionStage<List> doRxQueryAndInitializeNonLazyCollections(
			final SharedSessionContractImplementor session,
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
		CompletionStage<List>  result;
		try {
			try {
				result = doRxQuery( session, queryParameters, returnProxies, forcedResultTransformer );
			}
			finally {
				persistenceContext.afterLoad();
			}
			persistenceContext.initializeNonLazyCollections();
		}
		finally {
			// Restore the original default
			persistenceContext.setDefaultReadOnly( defaultReadOnlyOrig );
		}
		return result;
	}

	private CompletionStage<List> doRxQuery(
			final SharedSessionContractImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer) throws SQLException, HibernateException {

		final RowSelection selection = queryParameters.getRowSelection();
		final int maxRows = LimitHelper.hasMaxRows( selection ) ?
				selection.getMaxRows() :
				Integer.MAX_VALUE;

		final List<AfterLoadAction> afterLoadActions = new ArrayList<AfterLoadAction>();

		Function<ResultSet, Object> resultSetObjectFunction = resultSet -> {
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
				throw new HibernateException( ex );
			}
		};
		final CompletionStage<List> result = executeRxQueryStatement(
				getSQLString(), queryParameters, false, afterLoadActions, session,
				resultSetObjectFunction
		);
		return result;
	}

	protected CompletionStage<List> executeRxQueryStatement(
			String sqlStatement,
			QueryParameters queryParameters,
			boolean scroll,
			List<AfterLoadAction> afterLoadActions,
			SharedSessionContractImplementor session,
			Function<ResultSet, Object> transformer) throws SQLException {

		// Processing query filters.
		queryParameters.processFilters( sqlStatement, session );

		// Applying LIMIT clause.
		final LimitHandler limitHandler = getLimitHandler(
				queryParameters.getRowSelection()
		);
		String sql = limitHandler.processSql( queryParameters.getFilteredSQL(), queryParameters.getRowSelection() );

		// Adding locks and comments.
		sql = preprocessSQL( sql, queryParameters, getFactory(), afterLoadActions );

		RxQueryExecutor executor = new RxQueryExecutor();
		CompletionStage<List> result = executor.execute( sql, queryParameters, getFactory(), transformer );

		return result;
	}
}
