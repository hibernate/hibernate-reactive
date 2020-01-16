package org.hibernate.rx.persister.impl;

import org.hibernate.*;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.engine.spi.*;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.persister.entity.RxPersister;
import org.hibernate.persister.entity.SingleTableEntityPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.rx.loader.entity.impl.RxBatchingEntityLoaderBuilder;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public class RxSingleTableEntityPersister extends SingleTableEntityPersister implements RxEntityPersister {

	private RxPersister persister = new RxPersister(this);

	public RxSingleTableEntityPersister(
			PersistentClass persistentClass,
			EntityDataAccess cacheAccessStrategy,
			NaturalIdDataAccess naturalIdRegionAccessStrategy,
			PersisterCreationContext creationContext) throws HibernateException {
		super( persistentClass, cacheAccessStrategy, naturalIdRegionAccessStrategy, creationContext );
	}

	@Override
	protected String[] getIdentifierAliases() {
		return RxPersister.lower(super.getIdentifierAliases());
	}

	@Override
	public String[] getIdentifierAliases(String suffix) {
		return RxPersister.lower(super.getIdentifierAliases(suffix));
	}

	@Override
	public String[] getSubclassPropertyColumnAliases(String propertyName, String suffix) {
		return RxPersister.lower(super.getSubclassPropertyColumnAliases(propertyName, suffix));
	}

	@Override
	protected String[] getSubclassColumnAliasClosure() {
		return RxPersister.lower(super.getSubclassColumnAliasClosure());
	}

	@Override
	public String[] getPropertyAliases(String suffix, int i) {
		return RxPersister.lower(super.getPropertyAliases(suffix, i));
	}

	@Override
	public String getDiscriminatorAlias(String suffix) {
		return RxPersister.lower(super.getDiscriminatorAlias(suffix));
	}

	@Override
	protected String getDiscriminatorAlias() {
		return RxPersister.lower(super.getDiscriminatorAlias().toLowerCase());
	}

	@Override
	protected UniqueEntityLoader createEntityLoader(LockMode lockMode, LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		//FIXME add support to lock mode and loadQueryInfluencers
		return RxBatchingEntityLoaderBuilder.getBuilder( getFactory() )
				.buildLoader( this, batchSize, lockMode, getFactory(), loadQueryInfluencers );
	}


	@Override
	protected UniqueEntityLoader createEntityLoader(LockOptions lockOptions, LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		//FIXME add support to lock mode and loadQueryInfluencers
		return RxBatchingEntityLoaderBuilder.getBuilder( getFactory() )
				.buildLoader( this, batchSize, lockOptions, getFactory(), loadQueryInfluencers );
	}

	@Override
	public CompletionStage<?> insertRx(Serializable id, Object[] fields, Object object, SharedSessionContractImplementor session) {
		return persister.insertRx(id, fields, object, session);
	}

	@Override
	public CompletionStage<?> insertRx(Object[] fields, Object object, SharedSessionContractImplementor session) throws HibernateException {
		return persister.insertRx(fields, object, session);
	}

	@Override
	public CompletionStage<?> deleteRx(Serializable id, Object version, Object object, SharedSessionContractImplementor session) throws HibernateException {
		return persister.deleteRx(id, version, object, session);
	}

	@Override
	public CompletionStage<?> updateRx(Serializable id, Object[] fields, int[] dirtyFields, boolean hasDirtyCollection, Object[] oldFields, Object oldVersion, Object object, Object rowId, SharedSessionContractImplementor session) throws HibernateException {
		return persister.updateRx(id, fields, dirtyFields, hasDirtyCollection, oldFields, oldVersion, object, rowId, session);
	}

	@Override
	protected String generateInsertString(boolean identityInsert, boolean[] includeProperty, int j) {
		return RxPersister.fixSqlParameters( super.generateInsertString( identityInsert, includeProperty, j ) );
	}

	@Override
	protected String generateDeleteString(int j) {
		return RxPersister.fixSqlParameters( super.generateDeleteString(j) );
	}

	@Override
	protected String generateUpdateString(
			final boolean[] includeProperty,
			final int j,
			final Object[] oldFields,
			final boolean useRowId) {
		return RxPersister.fixSqlParameters( super.generateUpdateString(includeProperty, j, oldFields, useRowId) );
	}

	@Override
	protected Serializable insert(
			Object[] fields, boolean[] notNull, String sql, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	protected void insert(
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
	protected void delete(
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
	protected void updateOrInsert(
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
	protected boolean update(
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

}

