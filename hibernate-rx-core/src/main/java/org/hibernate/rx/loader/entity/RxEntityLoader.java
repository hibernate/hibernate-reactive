package org.hibernate.rx.loader.entity;

import java.io.Serializable;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.engine.jdbc.spi.JdbcCoordinator;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.entity.AbstractEntityLoader;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.Loadable;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

/**
 * @see org.hibernate.loader.entity.EntityLoader
 */
public class RxEntityLoader extends AbstractEntityLoader implements UniqueEntityLoader {

	// We don't use all the parameters but I kept them for simmetry with EntityLoader
	public RxEntityLoader(OuterJoinLoadable persister, int batchSize, LockOptions lockOptions, SessionFactoryImplementor factory, LoadQueryInfluencers loadQueryInfluencers) {
		this(persister, persister.getIdentifierType(), factory, loadQueryInfluencers);
	}

	// We don't use all the parameters but I kept them for simmetry with EntityLoader
	public RxEntityLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this(
				persister,
				persister.getIdentifierType(),
				factory,
				loadQueryInfluencers
		);
	}

	public RxEntityLoader(OuterJoinLoadable persister, Type identifierType, SessionFactoryImplementor factory, LoadQueryInfluencers influencers) {
		super( persister, identifierType, factory, influencers);
		if ( persister == null ) {
			throw new AssertionFailure( "EntityPersister must not be null or empty" );
		}
	}

	@Override
	public Object load(
			Serializable id, Object optionalObject, SharedSessionContractImplementor session)
			throws HibernateException {
		return load( id, optionalObject, session, LockOptions.NONE );
	}

	@Override
	public Object load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, LockOptions lockOptions) {
		return load( session, id, optionalObject, id, lockOptions );
	}

	@Override
	protected Object load(
			SharedSessionContractImplementor session,
			Object id,
			Object optionalObject,
			Serializable optionalId,
			LockOptions lockOptions) {

		Object list = loadRxEntity(
				session,
				id,
				uniqueKeyType,
				optionalObject,
				entityName,
				optionalId,
				persister,
				lockOptions
		);

// TODO: We need to validate the result
//		if ( list.size()==1 ) {
//			return list.get(0);
//		}
//		else if ( list.size()==0 ) {
//			return null;
//		}
//		else {
//			if ( getCollectionOwners()!=null ) {
//				return list.get(0);
//			}
//			else {
//				throw new HibernateException(
//						"More than one row with the given identifier was found: " +
//								id +
//								", for class: " +
//								persister.getEntityName()
//				);
//			}
//		}

		return list;
	}

	protected Object loadRxEntity(
			final SharedSessionContractImplementor session,
			final Object id,
			final Type identifierType,
			final Object optionalObject,
			final String optionalEntityName,
			final Serializable optionalIdentifier,
			final EntityPersister persister,
			LockOptions lockOptions) throws HibernateException {

		Object result;
		try {
			QueryParameters qp = new QueryParameters();
			qp.setPositionalParameterTypes( new Type[] {identifierType} );
			qp.setPositionalParameterValues( new Object[] {id} );
			qp.setOptionalObject( optionalObject );
			qp.setOptionalEntityName( optionalEntityName );
			qp.setOptionalId( optionalIdentifier );
			qp.setLockOptions( lockOptions );

			result = doRxQueryAndInitializeNonLazyCollections( session, qp, false );
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

		LOG.debug( "Done entity load" );
		return result;
	}

	public Object doRxQueryAndInitializeNonLazyCollections(
			final SharedSessionContractImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies)
			throws HibernateException, SQLException {
		return doRxQueryAndInitializeNonLazyCollections( session, queryParameters, returnProxies, null );
	}

	public Object doRxQueryAndInitializeNonLazyCollections(
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
		Object result;
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

	private Object doRxQuery(
			final SharedSessionContractImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer) throws SQLException, HibernateException {

		final RowSelection selection = queryParameters.getRowSelection();
		final int maxRows = LimitHelper.hasMaxRows( selection ) ?
				selection.getMaxRows() :
				Integer.MAX_VALUE;

		final List<AfterLoadAction> afterLoadActions = new ArrayList<AfterLoadAction>();

		final SqlStatementWrapper wrapper = executeRxQueryStatement( queryParameters, false, afterLoadActions, session );
		final ResultSet rs = wrapper.getResultSet();
		final Statement st = wrapper.getStatement();

		try {
			return processResultSet(
					rs,
					queryParameters,
					session,
					returnProxies,
					forcedResultTransformer,
					maxRows,
					afterLoadActions
			);
		}
		finally {
			final JdbcCoordinator jdbcCoordinator = session.getJdbcCoordinator();
			jdbcCoordinator.getLogicalConnection().getResourceRegistry().release( st );
			jdbcCoordinator.afterStatementExecution();
		}
	}

	protected SqlStatementWrapper executeRxQueryStatement(
			final QueryParameters queryParameters,
			final boolean scroll,
			List<AfterLoadAction> afterLoadActions,
			final SharedSessionContractImplementor session) throws SQLException {
		return executeRxQueryStatement( getSQLString(), queryParameters, scroll, afterLoadActions, session );
	}

	protected SqlStatementWrapper executeRxQueryStatement(
			String sqlStatement,
			QueryParameters queryParameters,
			boolean scroll,
			List<AfterLoadAction> afterLoadActions,
			SharedSessionContractImplementor session) throws SQLException {

		// Processing query filters.
		queryParameters.processFilters( sqlStatement, session );

		// Applying LIMIT clause.
		final LimitHandler limitHandler = getLimitHandler(
				queryParameters.getRowSelection()
		);
		String sql = limitHandler.processSql( queryParameters.getFilteredSQL(), queryParameters.getRowSelection() );

		// Adding locks and comments.
		sql = preprocessSQL( sql, queryParameters, getFactory(), afterLoadActions );

		final PreparedStatement st = prepareQueryStatement( sql, queryParameters, limitHandler, scroll, session );

		final ResultSet rs;

		if( queryParameters.isCallable() ) { //&& isTypeOf( st, CallableStatement.class ) ) {
			final CallableStatement cs = st.unwrap( CallableStatement.class );

			rs = getResultSet(
					cs,
					queryParameters.getRowSelection(),
					limitHandler,
					queryParameters.hasAutoDiscoverScalarTypes(),
					session
			);
		}
		else {
			rs = getResultSet(
					st,
					queryParameters.getRowSelection(),
					limitHandler,
					queryParameters.hasAutoDiscoverScalarTypes(),
					session
			);
		}

		return new SqlStatementWrapper(
				st,
				rs
		);
	}

	/**
	 * Wrapper class for {@link Statement} and associated {@link ResultSet}.
	 */
	protected static class SqlStatementWrapper {
		private final Statement statement;
		private final ResultSet resultSet;

		private SqlStatementWrapper(Statement statement, ResultSet resultSet) {
			this.resultSet = resultSet;
			this.statement = statement;
		}

		public ResultSet getResultSet() {
			return resultSet;
		}

		public Statement getStatement() {
			return statement;
		}
	}
}
