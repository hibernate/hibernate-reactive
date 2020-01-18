package org.hibernate.rx.persister.entity.impl;

import org.hibernate.*;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.engine.spi.*;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.persister.entity.UnionSubclassEntityPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.rx.loader.entity.impl.RxBatchingEntityLoaderBuilder;

import java.io.Serializable;

public class RxUnionSubclassEntityPersister extends UnionSubclassEntityPersister implements RxGeneratedIdentifierPersister {

	private RxIdentifierGenerator identifierGenerator;

	@Override
	public RxIdentifierGenerator getRxIdentifierGenerator() {
		return identifierGenerator;
	}

	public RxUnionSubclassEntityPersister(
			PersistentClass persistentClass,
			EntityDataAccess cacheAccessStrategy,
			NaturalIdDataAccess naturalIdRegionAccessStrategy,
			PersisterCreationContext creationContext) throws HibernateException {
		super( persistentClass, cacheAccessStrategy, naturalIdRegionAccessStrategy, creationContext );

		identifierGenerator = IdentifierGeneration.asRxGenerator( persistentClass, creationContext, getIdentifierGenerator() );
	}

	@Override
	public String[] getIdentifierAliases() {
		return PersisterUtil.lower(super.getIdentifierAliases());
	}

	@Override
	public String[] getIdentifierAliases(String suffix) {
		return PersisterUtil.lower(super.getIdentifierAliases(suffix));
	}

	@Override
	public String[] getSubclassPropertyColumnAliases(String propertyName, String suffix) {
		return PersisterUtil.lower(super.getSubclassPropertyColumnAliases(propertyName, suffix));
	}

	@Override
	public String[] getSubclassColumnAliasClosure() {
		return PersisterUtil.lower(super.getSubclassColumnAliasClosure());
	}

	@Override
	public String[] getPropertyAliases(String suffix, int i) {
		return PersisterUtil.lower(super.getPropertyAliases(suffix, i));
	}

	@Override
	public String getDiscriminatorAlias(String suffix) {
		return PersisterUtil.lower(super.getDiscriminatorAlias(suffix));
	}

	@Override
	public String getDiscriminatorAlias() {
		return PersisterUtil.lower(super.getDiscriminatorAlias().toLowerCase());
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
	public String generateInsertString(boolean identityInsert, boolean[] includeProperty, int j) {
		return PersisterUtil.fixSqlParameters( super.generateInsertString( identityInsert, includeProperty, j ) );
	}

	@Override
	public String generateDeleteString(int j) {
		return PersisterUtil.fixSqlParameters( super.generateDeleteString(j) );
	}

	@Override
	public String generateUpdateString(
			final boolean[] includeProperty,
			final int j,
			final Object[] oldFields,
			final boolean useRowId) {
		return PersisterUtil.fixSqlParameters( super.generateUpdateString(includeProperty, j, oldFields, useRowId) );
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

}

