package org.hibernate.reactive.loader.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.Loadable;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.reactive.loader.ReactiveOuterJoinLoader;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @see org.hibernate.loader.entity.AbstractEntityLoader
 */
public abstract class ReactiveAbstractEntityLoader extends ReactiveOuterJoinLoader implements ReactiveUniqueEntityLoader {

	protected final OuterJoinLoadable persister;
	protected final Type uniqueKeyType;
	protected final String entityName;

	protected ReactiveAbstractEntityLoader(
			OuterJoinLoadable persister,
			Type uniqueKeyType,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) {
		super( factory, loadQueryInfluencers );
		this.uniqueKeyType = uniqueKeyType;
		this.entityName = persister.getEntityName();
		this.persister = persister;
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
				if (e instanceof JDBCException) {
					final Loadable[] persisters = getEntityPersisters();
					throw this.getFactory().getJdbcServices().getSqlExceptionHelper().convert(
							((JDBCException) e).getSQLException(),
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

	@Override
	protected Object getResultColumnOrRow(
			Object[] row,
			ResultTransformer transformer,
			ResultSet rs,
			SharedSessionContractImplementor session) {
		return row[ row.length - 1 ];
	}

	@Override
	protected boolean isSingleRowLoader() {
		return true;
	}
}
