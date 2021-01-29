/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.entity.impl;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.JoinWalker;
import org.hibernate.loader.entity.AbstractEntityLoader;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.Loadable;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.reactive.loader.ReactiveLoaderBasedLoader;
import org.hibernate.reactive.loader.ReactiveLoaderBasedResultSetProcessor;
import org.hibernate.reactive.loader.ReactiveResultSetProcessor;
import org.hibernate.reactive.loader.entity.ReactiveUniqueEntityLoader;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.util.impl.CompletionStages.logSqlException;
import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;

/**
 * A reactific {@link org.hibernate.loader.entity.AbstractEntityLoader}.
 *
 * @see org.hibernate.loader.entity.AbstractEntityLoader
 */
public abstract class ReactiveAbstractEntityLoader extends AbstractEntityLoader
		implements ReactiveUniqueEntityLoader, ReactiveLoaderBasedLoader {

	private final ReactiveLoaderBasedResultSetProcessor resultSetProcessor;
	private final Parameters parameters;

	protected ReactiveAbstractEntityLoader(
			OuterJoinLoadable persister,
			Type uniqueKeyType,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) {
		super( persister, uniqueKeyType, factory, loadQueryInfluencers );
		resultSetProcessor = new ReactiveLoaderBasedResultSetProcessor( this );
		parameters = Parameters.instance( factory.getJdbcServices().getDialect() );
	}

	@Override
	protected void initFromWalker(JoinWalker walker) {
		String processedSQLString = parameters().process( walker.getSQLString() );
		walker.setSql( processedSQLString );
		super.initFromWalker( walker );
	}

	@Override
	public Parameters parameters() {
		return parameters;
	}

	protected CompletionStage<List<Object>> doReactiveQueryAndInitializeNonLazyCollections(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies) {
		return doReactiveQueryAndInitializeNonLazyCollections(
				getSQLString(),
				session,
				queryParameters,
				returnProxies,
				null
		);
	}

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
			LockOptions lockOptions) throws HibernateException {

		QueryParameters parameters = buildQueryParameters(
				id, identifierType,
				optionalObject,
				optionalEntityName,
				optionalIdentifier,
				lockOptions
		);

		return doReactiveQueryAndInitializeNonLazyCollections( session, parameters, false )
			.handle( (list, err) -> {
				LOG.debug( "Done entity load" );
				Loadable[] persisters = getEntityPersisters();
				logSqlException( err,
						() -> "could not load an entity: " +
								infoString(
										persisters[persisters.length - 1],
										id,
										identifierType,
										getFactory()
								),
						getSQLString()
				);
				return returnOrRethrow( err, list );
			} );
	}

	private QueryParameters buildQueryParameters(Object id, Type identifierType,
												 Object optionalObject,
												 String optionalEntityName,
												 Serializable optionalIdentifier,
												 LockOptions lockOptions) {
		QueryParameters parameters = new QueryParameters();
		parameters.setPositionalParameterTypes( new Type[] { identifierType } );
		parameters.setPositionalParameterValues( new Object[] { id } );
		parameters.setOptionalObject( optionalObject );
		parameters.setOptionalEntityName( optionalEntityName );
		parameters.setOptionalId( optionalIdentifier );
		parameters.setLockOptions( lockOptions );
		return parameters;
	}

	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject,
										SharedSessionContractImplementor session) {
		// this form is deprecated!
		return load( id, optionalObject, session, LockOptions.NONE, null );
	}

	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject,
										SharedSessionContractImplementor session,
										Boolean readOnly) {
		// this form is deprecated!
		return load( id, optionalObject, session, LockOptions.NONE, readOnly );
	}

	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject,
										SharedSessionContractImplementor session,
										LockOptions lockOptions) {
		return load( id, optionalObject, session, lockOptions, null );
	}

	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject,
										SharedSessionContractImplementor session,
										LockOptions lockOptions, Boolean readOnly) {
		return load( session, id, optionalObject, id, lockOptions, readOnly );
	}

	@Override
	public String preprocessSQL(String sql,
								QueryParameters queryParameters,
								SessionFactoryImplementor factory,
								List<AfterLoadAction> afterLoadActions) {
		return super.preprocessSQL(sql, queryParameters, factory, afterLoadActions);
	}

	@Override
	public List<Object> processResultSet(ResultSet resultSet,
										 QueryParameters queryParameters,
										 SharedSessionContractImplementor session,
										 boolean returnProxies,
										 ResultTransformer forcedResultTransformer,
										 int maxRows, List<AfterLoadAction> afterLoadActions) throws SQLException {
		throw new UnsupportedOperationException( "use #reactiveProcessResultSet instead." );
	}

	@Override
	public ReactiveResultSetProcessor getReactiveResultSetProcessor() {
		return resultSetProcessor;
	}

	@Override
	public boolean isSubselectLoadingEnabled() {
		return super.isSubselectLoadingEnabled();
	}

	@Override
	public List<Object> getRowsFromResultSet(
			ResultSet rs,
			QueryParameters queryParameters,
			SharedSessionContractImplementor session,
			boolean returnProxies,
			ResultTransformer forcedResultTransformer,
			int maxRows,
			List<Object> hydratedObjects,
			List<EntityKey[]> subselectResultKeys) throws SQLException {
		return super.getRowsFromResultSet(
				rs,
				queryParameters,
				session,
				returnProxies,
				forcedResultTransformer,
				maxRows,
				hydratedObjects,
				subselectResultKeys
		);
	}

	@Override
	public void createSubselects(
			final List keys,
			final QueryParameters queryParameters,
			final SharedSessionContractImplementor session) {
		super.createSubselects( keys, queryParameters, session );
	}

	@Override
	public void endCollectionLoad(Object resultSetId, SharedSessionContractImplementor session, CollectionPersister collectionPersister) {
		super.endCollectionLoad( resultSetId, session, collectionPersister );
	}

	@Override
	public List<Object> doQueryAndInitializeNonLazyCollections(SharedSessionContractImplementor session, QueryParameters queryParameters, boolean returnProxies) throws HibernateException, SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<Object> doQueryAndInitializeNonLazyCollections(SharedSessionContractImplementor session, QueryParameters queryParameters, boolean returnProxies, ResultTransformer forcedResultTransformer) throws HibernateException, SQLException {
		throw new UnsupportedOperationException();
	}
}
