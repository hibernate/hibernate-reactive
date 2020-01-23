package org.hibernate.rx.loader.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import java.sql.SQLException;
import java.util.List;

/**
 * @see org.hibernate.loader.entity.EntityLoader
 */
public class RxEntityLoader extends RxAbstractEntityLoader implements UniqueEntityLoader {

	private final int[][] compositeKeyManyToOneTargetIndices;

	public RxEntityLoader(
			OuterJoinLoadable persister,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this( persister, 1, LockMode.NONE, factory, loadQueryInfluencers );
	}
//
//	// We don't use all the parameters but I kept them for symmetry with EntityLoader
//	public RxEntityLoader(
//			OuterJoinLoadable persister,
//			LockMode lockMode,
//			SessionFactoryImplementor factory,
//			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
//		this( persister, 1, lockMode, factory, loadQueryInfluencers );
//	}
//
//	public RxEntityLoader(
//			OuterJoinLoadable persister,
//			LockOptions lockOptions,
//			SessionFactoryImplementor factory,
//			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
//		this( persister, 1, lockOptions, factory, loadQueryInfluencers );
//	}

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
				new RxEntityJoinWalker(
						persister,
						uniqueKey,
						batchSize,
						lockMode,
						factory,
						loadQueryInfluencers
				) );

		if ( LOG.isDebugEnabled() ) {
			LOG.debugf( "Static select for entity %s [%s]: %s", entityName, lockMode, getSQLString() );
		}

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
				new RxEntityJoinWalker(
						persister,
						uniqueKey,
						batchSize,
						lockOptions,
						factory,
						loadQueryInfluencers
				) );

		if ( LOG.isDebugEnabled() ) {
			LOG.debugf( "Static select for entity %s [%s:%s]: %s",
					entityName,
					lockOptions.getLockMode(),
					lockOptions.getTimeOut(),
					getSQLString() );
		}
	}

	private RxEntityLoader(
			OuterJoinLoadable persister,
			String[] uniqueKey,
			Type uniqueKeyType,
			int batchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers,
			org.hibernate.loader.entity.EntityJoinWalker walker) throws MappingException {
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
	public List<?> doQueryAndInitializeNonLazyCollections(SharedSessionContractImplementor session, QueryParameters queryParameters, boolean returnProxies) throws HibernateException, SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<?> doQueryAndInitializeNonLazyCollections(SharedSessionContractImplementor session, QueryParameters queryParameters, boolean returnProxies, ResultTransformer forcedResultTransformer) throws HibernateException, SQLException {
		throw new UnsupportedOperationException();
	}
}
