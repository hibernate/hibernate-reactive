package org.hibernate.rx.persister.impl;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.Session;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.jdbc.batch.internal.BasicBatchKey;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.jdbc.Expectation;
import org.hibernate.jdbc.Expectations;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.SingleTableEntityPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.rx.impl.RxQueryExecutor;
import org.hibernate.rx.loader.entity.impl.RxBatchingEntityLoaderBuilder;
import org.hibernate.tuple.InMemoryValueGenerationStrategy;

public class RxSingleTableEntityPersister extends SingleTableEntityPersister implements EntityPersister {

	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( RxSingleTableEntityPersister.class );

	private BasicBatchKey inserBatchKey;
	private final RxQueryExecutor queryExecutor = new RxQueryExecutor();

	public RxSingleTableEntityPersister(
			PersistentClass persistentClass,
			EntityDataAccess cacheAccessStrategy,
			NaturalIdDataAccess naturalIdRegionAccessStrategy,
			PersisterCreationContext creationContext) throws HibernateException {
		super( persistentClass, cacheAccessStrategy, naturalIdRegionAccessStrategy, creationContext );
	}

	@Override
	public Object load(Serializable id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session)
			throws HibernateException {

		final UniqueEntityLoader loader = getAppropriateLoader( lockOptions, session );
		return loader.load( id, optionalObject, session, lockOptions );
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
	protected UniqueEntityLoader createEntityLoader(LockMode lockMode) throws MappingException {
		return createEntityLoader( lockMode, LoadQueryInfluencers.NONE );
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

	private void preInsertInMemoryValueGeneration(Object[] fields, Object object, SharedSessionContractImplementor session) {
		if ( getEntityMetamodel().hasPreInsertGeneratedValues() ) {
			final InMemoryValueGenerationStrategy[] strategies = getEntityMetamodel().getInMemoryValueGenerationStrategies();
			for ( int i = 0; i < strategies.length; i++ ) {
				if ( strategies[i] != null && strategies[i].getGenerationTiming().includesInsert() ) {
					fields[i] = strategies[i].getValueGenerator().generateValue( (Session) session, object );
					setPropertyValue( object, i, fields[i] );
				}
			}
		}
	}

	public CompletionStage<?> insertRx(Serializable id, Object[] fields, Object object, SharedSessionContractImplementor session) {
		// apply any pre-insert in-memory value generation
		preInsertInMemoryValueGeneration( fields, object, session );

		CompletionStage<?> insertStage = null;
		final int span = getTableSpan();
		if ( getEntityMetamodel().isDynamicInsert() ) {
			// For the case of dynamic-insert="true", we need to generate the INSERT SQL
			boolean[] notNull = getPropertiesToInsert( fields );
			for ( int j = 0; j < span; j++ ) {
				insertStage = insertRx( id, fields, notNull, j, generateInsertString( notNull, j ), object, session );
			}
		}
		else {
			// For the case of dynamic-insert="false", use the static SQL
			for ( int j = 0; j < span; j++ ) {
				insertStage = insertRx( id, fields, getPropertyInsertability(), j, getSQLInsertStrings()[j], object, session );
			}
		}
		return insertStage;
	}

	// Should it return the id?
	public CompletionStage<?> insertRx(Object[] fields, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		// apply any pre-insert in-memory value generation
		preInsertInMemoryValueGeneration( fields, object, session );

		CompletionStage<?> insertStage = null;
		final int span = getTableSpan();
		final Serializable id;
		if ( getEntityMetamodel().isDynamicInsert() ) {
			// For the case of dynamic-insert="true", we need to generate the INSERT SQL
			boolean[] notNull = getPropertiesToInsert( fields );
			id = insert( fields, notNull, generateInsertString( true, notNull ), object, session );
			for ( int j = 1; j < span; j++ ) {
				insertStage = insertRx( id, fields, notNull, j, generateInsertString( notNull, j ), object, session );
			}
		}
		else {
			// For the case of dynamic-insert="false", use the static SQL
			id = insert( fields, getPropertyInsertability(), getSQLIdentityInsertString(), object, session );
			for ( int j = 1; j < span; j++ ) {
				insertStage = insertRx( id, fields, getPropertyInsertability(), j, getSQLInsertStrings()[j], object, session );
			}
		}
		return insertStage;
	}

	public CompletionStage<?> insertRx(
			Serializable id,
			Object[] fields,
			boolean[] notNull,
			int j,
			String sql,
			Object object,
			SharedSessionContractImplementor session) throws HibernateException {

		if ( isInverseTable( j ) ) {
			return CompletableFuture.completedFuture( null );
		}

		//note: it is conceptually possible that a UserType could map null to
		//	  a non-null value, so the following is arguable:
		if ( isNullableTable( j ) && isAllNull( fields, j ) ) {
			return CompletableFuture.completedFuture( null );
		}

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev( "Inserting entity: {0}", MessageHelper.infoString( this, id, getFactory() ) );
			if ( j == 0 && isVersioned() ) {
				LOG.tracev( "Version: {0}", Versioning.getVersion( fields, this ) );
			}
		}

		// TODO : shouldn't inserts be Expectations.NONE?
		final Expectation expectation = Expectations.appropriateExpectation( insertResultCheckStyles[j] );
		final int jdbcBatchSizeToUse = session.getConfiguredJdbcBatchSize();
		final boolean useBatch = expectation.canBeBatched() &&
				jdbcBatchSizeToUse > 1 &&
				getIdentifierGenerator().supportsJdbcBatchInserts();

		if ( useBatch && inserBatchKey == null ) {
			inserBatchKey = new BasicBatchKey(
					getEntityName() + "#INSERT",
					expectation
			);
		}
		final boolean callable = isInsertCallable( j );

		Object[] paramValues = paramValues( id, fields );
		CompletionStage<Object> insertStage = queryExecutor.update( sql, paramValues, getFactory() );
		return insertStage;
	}

	// FIXME: Just to make the test works while I figure out the dehydrate method
	private Object[] paramValues(Serializable id, Object[] fields) {
		Object[] paramValues = new Object[fields.length + 1];
		if ( id != null ) {
			for ( int i = 0; i < fields.length; i++ ) {
				paramValues[i] = fields[i];
			}
			paramValues[fields.length] = id;
		}
		return paramValues;
	}

	@Override
	protected String generateInsertString(boolean identityInsert, boolean[] includeProperty, int j) {
		final String VALUES = ") values (";
		String insertSQL = super.generateInsertString( identityInsert, includeProperty, j );
		int parametersStartIndex = insertSQL.indexOf( VALUES ) + VALUES.length();
		long paramNum = insertSQL.substring( parametersStartIndex ).chars().filter( ch -> ch == '?' ).count();
		String firstPart = insertSQL.substring( 0, parametersStartIndex );
		StringBuilder builder = new StringBuilder();
		for ( int i = 1; i < paramNum + 1; i++ ) {
			builder.append( ", $" );
			builder.append( i );
		}
		builder.append( ")" );
		insertSQL = firstPart + builder.substring( 2 );
		return insertSQL;
	}
}

