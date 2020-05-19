package org.hibernate.reactive.persister.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.engine.spi.CascadingActions;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.persister.entity.MultiLoadOptions;
import org.hibernate.persister.entity.SingleTableEntityPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.reactive.loader.entity.impl.ReactiveAbstractEntityLoader;
import org.hibernate.reactive.loader.entity.impl.ReactiveCascadeEntityLoader;
import org.hibernate.reactive.loader.entity.impl.ReactiveBatchingEntityLoaderBuilder;
import org.hibernate.reactive.sql.impl.Delete;
import org.hibernate.reactive.sql.impl.Insert;
import org.hibernate.reactive.sql.impl.Parameters;
import org.hibernate.reactive.sql.impl.Update;

import java.io.Serializable;
import java.util.List;

/**
 * An {@link ReactiveEntityPersister} backed by {@link SingleTableEntityPersister}
 *  * amd {@link ReactiveAbstractEntityPersister}.
 */
public class ReactiveSingleTableEntityPersister extends SingleTableEntityPersister implements ReactiveAbstractEntityPersister {

	private ReactiveIdentifierGenerator identifierGenerator;

	@Override
	public ReactiveIdentifierGenerator getReactiveIdentifierGenerator() {
		return identifierGenerator;
	}

	public ReactiveSingleTableEntityPersister(
			PersistentClass persistentClass,
			EntityDataAccess cacheAccessStrategy,
			NaturalIdDataAccess naturalIdRegionAccessStrategy,
			PersisterCreationContext creationContext) throws HibernateException {
		super( persistentClass, cacheAccessStrategy, naturalIdRegionAccessStrategy, creationContext );

		identifierGenerator = IdentifierGeneration.asReactiveGenerator( persistentClass, creationContext, getIdentifierGenerator() );
	}

	@Override
	protected void createLoaders() {
		super.createLoaders();

		getLoaders().put( "merge", new ReactiveCascadeEntityLoader( this, CascadingActions.MERGE, getFactory() ) );
		getLoaders().put( "refresh", new ReactiveCascadeEntityLoader( this, CascadingActions.REFRESH, getFactory() ) );
	}

	@Override
	protected UniqueEntityLoader createEntityLoader(LockMode lockMode, LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		//FIXME add support to lock mode and loadQueryInfluencers
		return ReactiveBatchingEntityLoaderBuilder.getBuilder( getFactory() )
				.buildLoader( this, batchSize, lockMode, getFactory(), loadQueryInfluencers );
	}

	@Override
	protected UniqueEntityLoader createEntityLoader(LockOptions lockOptions, LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		//FIXME add support to lock mode and loadQueryInfluencers
		return ReactiveBatchingEntityLoaderBuilder.getBuilder( getFactory() )
				.buildLoader( this, batchSize, lockOptions, getFactory(), loadQueryInfluencers );
	}

	@Override
	protected Update createUpdate() {
		return new Update( getFactory().getJdbcServices().getDialect(),
				Parameters.createDialectParameterGenerator( getFactory() ) );
	}

	@Override
	protected Insert createInsert() {
		return new Insert( getFactory().getJdbcServices().getDialect(),
				Parameters.createDialectParameterGenerator( getFactory() ) );
	}

	@Override
	protected Delete createDelete() {
		return new Delete( Parameters.createDialectParameterGenerator( getFactory() ) );
	}

	@Override
	public Serializable insert(
			Object[] fields, boolean[] notNull, String sql, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void insert(
			Serializable id,
			Object[] fields,
			boolean[] notNull,
			int j,
			String sql,
			Object object,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public Serializable insert(
			Object[] fields, Object object, SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void insert(Serializable id, Object[] fields, Object object, SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void delete(
			Serializable id,
			Object version,
			int j,
			Object object,
			String sql,
			SharedSessionContractImplementor session,
			Object[] loadedState) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void delete(
			Serializable id, Object version, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void updateOrInsert(
			Serializable id,
			Object[] fields,
			Object[] oldFields,
			Object rowId,
			boolean[] includeProperty,
			int j,
			Object oldVersion,
			Object object,
			String sql,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public boolean update(
			Serializable id,
			Object[] fields,
			Object[] oldFields,
			Object rowId,
			boolean[] includeProperty,
			int j,
			Object oldVersion,
			Object object,
			String sql,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void update(
			Serializable id,
			Object[] fields,
			int[] dirtyFields,
			boolean hasDirtyCollection,
			Object[] oldFields,
			Object oldVersion,
			Object object,
			Object rowId,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public List multiLoad(Serializable[] ids, SharedSessionContractImplementor session, MultiLoadOptions loadOptions) {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public ReactiveAbstractEntityLoader getAppropriateLoader(LockOptions lockOptions, SharedSessionContractImplementor session) {
		return (ReactiveAbstractEntityLoader) super.getAppropriateLoader(lockOptions, session);
	}
}

