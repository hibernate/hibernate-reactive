/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.StaleObjectStateException;
import org.hibernate.bytecode.enhance.spi.LazyPropertyInitializer;
import org.hibernate.bytecode.enhance.spi.interceptor.BytecodeLazyAttributeInterceptor;
import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.bytecode.enhance.spi.interceptor.LazyAttributeDescriptor;
import org.hibernate.bytecode.enhance.spi.interceptor.LazyAttributeLoadingInterceptor;
import org.hibernate.bytecode.spi.BytecodeEnhancementMetadata;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.internal.ManagedTypeHelper;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.PersistentAttributeInterceptor;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.jdbc.Expectation;
import org.hibernate.loader.ast.internal.SingleIdArrayLoadPlan;
import org.hibernate.loader.entity.CacheEntityLoaderHelper;
import org.hibernate.metamodel.mapping.EntityVersionMapping;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleIdEntityLoader;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.session.impl.ReactiveQueryExecutorLookup;
import org.hibernate.sql.SimpleSelect;
import org.hibernate.sql.Update;
import org.hibernate.type.BasicType;

import jakarta.persistence.metamodel.Attribute;

import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.logSqlException;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * An abstract implementation of {@link ReactiveEntityPersister} whose
 * concrete implementations each extend a concrete subclass of
 * {@link AbstractEntityPersister}. Note that there are three
 * main flavors of {@code AbstractEntityPersister}, one for each
 * of the three inheritance mapping strategies, and thus we have
 * three flavors of {@link ReactiveEntityPersister}. Therefore, this
 * interface is defined as a mixin. This design avoid duplicating
 * the code in this class in the three different subclasses.
 *
 * Concrete implementations of this interface _must_ also extend
 * {@code AbstractEntityPersister} or one of its concrete
 * subclasses.
 *
 * @see AbstractEntityPersister
 * @see ReactiveJoinedSubclassEntityPersister
 * @see ReactiveUnionSubclassEntityPersister
 * @see ReactiveSingleTableEntityPersister
 */
public interface ReactiveAbstractEntityPersister extends ReactiveEntityPersister, OuterJoinLoadable, Lockable {

	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	default Parameters parameters() {
		return Parameters.instance( getFactory().getJdbcServices().getDialect() );
	}

	/**
	 * A self-reference of type {@code AbstractEntityPersister}.
	 *
	 * @return this object
	 */
	default AbstractEntityPersister delegate() {
		return (AbstractEntityPersister) this;
	}

	default ReactiveConnection getReactiveConnection(SharedSessionContractImplementor session) {
		return ReactiveQueryExecutorLookup.extract( session ).getReactiveConnection();
	}

	boolean check(
			int rows,
			Object id,
			int tableNumber,
			Expectation expectation,
			PreparedStatement statement, String sql) throws HibernateException;

	default String generateSelectLockString(LockOptions lockOptions) {
		final SessionFactoryImplementor factory = getFactory();
		Dialect dialect = factory.getJdbcServices().getDialect();
		final SimpleSelect select = new SimpleSelect(dialect)
				.setLockOptions( lockOptions )
				.setTableName( getRootTableName() )
				.addColumn( getRootTableIdentifierColumnNames()[0] )
				.addCondition( getRootTableIdentifierColumnNames(), "=?" );
		if ( isVersioned() ) {
			select.addCondition( getVersionColumnName(), "=?" );
		}
		if ( factory.getSessionFactoryOptions().isCommentsEnabled() ) {
			select.setComment( lockOptions.getLockMode() + " lock " + getEntityName() );
		}
		return parameters().process( select.toStatementString() );
	}

	default String generateUpdateLockString(LockOptions lockOptions) {
		final SessionFactoryImplementor factory = getFactory();
		Dialect dialect = factory.getJdbcServices().getDialect();
		final Update update = new Update(dialect);
		update.setTableName( getRootTableName() );
		update.addPrimaryKeyColumns( getRootTableIdentifierColumnNames() );
		update.setVersionColumnName( getVersionColumnName() );
		update.addColumn( getVersionColumnName() );
		if ( factory.getSessionFactoryOptions().isCommentsEnabled() ) {
			update.setComment( lockOptions.getLockMode() + " lock " + getEntityName() );
		}
		return parameters().process( update.toStatementString() );
	}

	@Override
	default CompletionStage<Void> reactiveLock(
			Object id,
			Object version,
			Object object,
			LockOptions lockOptions,
			SharedSessionContractImplementor session)
			throws HibernateException {

		LockMode lockMode = lockOptions.getLockMode();

		Object nextVersion = nextVersionForLock( lockMode, id, version, object, session );

		String sql;
		boolean writeLock;
		switch (lockMode) {
			// 0) noop
			case NONE:
				return voidFuture();
			// 1) select ... for share
			case PESSIMISTIC_READ:
			// 2) select ... for update
			case PESSIMISTIC_WRITE:
			// 3) select ... for nowait
			case UPGRADE_NOWAIT:
			// 4) select ... for update skip locked
			case UPGRADE_SKIPLOCKED:
				// TODO: introduce separate support for PESSIMISTIC_READ
				// the current implementation puts the version number in
				// the where clause and the id in the select list, whereas
				// it would be better to actually select and check the
				// version number (same problem in hibernate-core)
				sql = generateSelectLockString( lockOptions );
				writeLock = false;
				break;
			// 5) update ... set version
			case PESSIMISTIC_FORCE_INCREMENT:
				sql = generateUpdateLockString( lockOptions );
				writeLock = true;
				break;
			// 6) OPTIMISTIC locks are converted to pessimistic
			//    locks obtained in the before completion phase
			case OPTIMISTIC:
			case OPTIMISTIC_FORCE_INCREMENT:
				throw new AssertionFailure("optimistic lock mode is not supported here");
			// 7) READ and WRITE are obtained implicitly by
			//    other operations
			case READ:
			case WRITE:
				throw new AssertionFailure("implicit lock mode is not supported here");
			default:
				throw new AssertionFailure("illegal lock mode");
		}

		Object[] arguments = PreparedStatementAdaptor.bind( statement -> {
			int offset = 1;
			if ( writeLock ) {
				getVersionType().nullSafeSet( statement, nextVersion, offset, session );
				offset++;
			}
			getIdentifierType().nullSafeSet( statement, id, offset, session );
			offset += getIdentifierType().getColumnSpan( getFactory() );
			if ( isVersioned() ) {
				getVersionType().nullSafeSet( statement, version, offset, session );
			}
		} );

		return writeLock( session, sql, writeLock, arguments )
				.thenAccept( rowExisted -> {
					if ( !rowExisted ) {
						throw new StaleObjectStateException( getEntityName(), id );
					}
				} )
				.whenComplete( (r, e) -> logSqlException( e, () -> "could not lock: " + infoString( this, id, getFactory() ), sql ) );
	}

	private CompletionStage<Boolean> writeLock(SharedSessionContractImplementor session, String sql, boolean writeLock, Object[] arguments) {
		return writeLock
				? getReactiveConnection( session ).update( sql, arguments ).thenApply( affected -> affected > 0 )
				: getReactiveConnection( session ).select( sql, arguments ).thenApply( Iterator::hasNext );
	}

	/**
	 * @see AbstractEntityPersister#getVersionType()
	 */
	@Override
	BasicType<?> getVersionType();

	/**
	 * @see AbstractEntityPersister#forceVersionIncrement(Object, Object, SharedSessionContractImplementor)
	 */
	default Object nextVersionForLock(LockMode lockMode, Object id, Object currentVersion, Object entity, SharedSessionContractImplementor session) {
		if ( lockMode == LockMode.PESSIMISTIC_FORCE_INCREMENT ) {
			if ( !isVersioned() ) {
				throw new AssertionFailure( "cannot force version increment on non-versioned entity" );
			}

			final EntityVersionMapping versionMapping = getVersionMapping();
			BasicType<?> versionType = getVersionType();
			final Object nextVersion = getVersionJavaType()
					.next( currentVersion, versionMapping.getLength(), versionMapping.getPrecision(), versionMapping.getScale(), session );

			if ( LOG.isTraceEnabled() ) {
				LOG.trace( "Forcing version increment [" + infoString( this, id, getFactory() ) + "; "
								+ versionType.toLoggableString( currentVersion, getFactory() ) + " -> "
								+ versionType.toLoggableString( nextVersion, getFactory() ) + "]" );
			}

			session.getPersistenceContextInternal().getEntry( entity ).forceLocked( entity, nextVersion );

			return nextVersion;
		}
		else {
			return currentVersion;
		}
	}

	@Override
	default CompletionStage<Object[]> reactiveGetDatabaseSnapshot(Object id, SharedSessionContractImplementor session) {
		return getReactiveSingleIdEntityLoader().reactiveLoadDatabaseSnapshot( id, session );
	}

	ReactiveSingleIdEntityLoader<?> getReactiveSingleIdEntityLoader();

	/**
	 * @see AbstractEntityPersister#getCurrentVersion(Object, SharedSessionContractImplementor)
	 */
	@Override
	default CompletionStage<Object> reactiveGetCurrentVersion(Object id, SharedSessionContractImplementor session) {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracev( "Getting version: {0}", infoString( this, id, getFactory() ) );
		}

		Object[] params = PreparedStatementAdaptor
				.bind( statement -> getIdentifierType().nullSafeSet( statement, id, 1, session ) );

		return getReactiveConnection( session )
				.selectJdbc( delegate().getVersionSelectString(), params )
				.thenCompose( resultSet -> currentVersion( session, resultSet ) );
	}

	private CompletionStage<Object> currentVersion(SharedSessionContractImplementor session, ResultSet resultSet) {
		try {
			if ( !resultSet.next() ) {
				return nullFuture();
			}
			if ( !isVersioned() ) {
				return completedFuture( this );
			}
			return completedFuture( getVersionType()
											.getJdbcMapping()
											.getJdbcValueExtractor()
											.extract( resultSet, 1, session ) );
		}
		catch (SQLException sqle) {
			//can never happen
			return failedFuture( new JDBCException( "error reading version", sqle ) );
		}
	}

	@SuppressWarnings("unchecked")
	default <E, T> CompletionStage<T> reactiveInitializeLazyProperty(Attribute<E, T> field, E entity, SharedSessionContractImplementor session) {
		String fieldName = field.getName();
		Object result = initializeLazyProperty( fieldName, entity, session );
		if (result instanceof CompletionStage) {
			return (CompletionStage<T>) result;
		}
		else if (result instanceof PersistentCollection) {
			// Hibernate core doesn't set the field when it's a
			// collection. That's inconsistent with what happens
			// for other lazy fields, so let's set the field here
			String[] propertyNames = getPropertyNames();
			for (int index=0; index<propertyNames.length; index++) {
				if ( propertyNames[index].equals(fieldName) ) {
					setPropertyValue( entity, index, result );
					break;
				}
			}

			// Hibernate core just instantiates a collection
			// wrapper but doesn't fetch it, since lazy fetching
			// is transparent there. That's too painful in our
			// case, since it would make the user have to call
			// fetch() twice, so fetch it here.
			PersistentCollection collection = (PersistentCollection) result;
			return collection.wasInitialized()
					? completedFuture( (T) collection )
					: ((ReactiveSession) session).reactiveInitializeCollection( collection, false )
							.thenApply( v -> (T) result );
		}
		else {
			return completedFuture( (T) result );
		}
	}

	// See AbstractEntityPersister#initializeLazyPropertiesFromDatastore(Object, Object, EntityEntry, String, SharedSessionContractImplementor)
	default CompletionStage<Object> reactiveInitializeLazyPropertiesFromDatastore(
			Object entity,
			Object id,
			EntityEntry entry,
			String fieldName,
			SharedSessionContractImplementor session) {

		if ( !hasLazyProperties() ) {
			throw new AssertionFailure( "no lazy properties" );
		}

		final PersistentAttributeInterceptor interceptor = ManagedTypeHelper.asPersistentAttributeInterceptable( entity ).$$_hibernate_getInterceptor();
		if ( interceptor == null ) {
			throw new AssertionFailure( "Expecting bytecode interceptor to be non-null" );
		}

		LOG.tracef( "Initializing lazy properties from datastore (triggered for `%s`)", fieldName );

		String fetchGroup = getEntityMetamodel()
				.getBytecodeEnhancementMetadata()
				.getLazyAttributesMetadata()
				.getFetchGroupName( fieldName );
		List<LazyAttributeDescriptor> fetchGroupAttributeDescriptors = getEntityMetamodel()
				.getBytecodeEnhancementMetadata()
				.getLazyAttributesMetadata()
				.getFetchGroupAttributeDescriptors( fetchGroup );

		@SuppressWarnings("deprecation")
		Set<String> initializedLazyAttributeNames = interceptor.getInitializedLazyAttributeNames();

		// FIXME: How do I pass this to the query?
		Object[] arguments = PreparedStatementAdaptor.bind(
				statement -> getIdentifierType().nullSafeSet( statement, id, 1, session )
		);

		final SingleIdArrayLoadPlan lazySelect = getSQLLazySelectLoadPlan( fetchGroup );

		// null sql means that the only lazy properties
		// are shared PK one-to-one associations which are
		// handled differently in the Type#nullSafeGet code...
		if ( lazySelect == null ) {
			return initLazyProperty(
					fieldName, entity,
					session, entry,
					interceptor,
					fetchGroupAttributeDescriptors,
					initializedLazyAttributeNames,
					null
			);
		}

		// FIXME: We need a reactive version SingleIdArrayLoadPlan
		return completedFuture( lazySelect.load( id, session ) );
	}

	default CompletionStage<Object> initLazyProperty(
			String fieldName, Object entity,
			SharedSessionContractImplementor session,
			EntityEntry entry,
			PersistentAttributeInterceptor interceptor,
			List<LazyAttributeDescriptor> fetchGroupAttributeDescriptors,
			Set<String> initializedLazyAttributeNames,
			Object[] values) {    // Load all the lazy properties that are in the same fetch group
		CompletionStage<Object> resultStage = nullFuture();
		int i = 0;
		for ( LazyAttributeDescriptor fetchGroupAttributeDescriptor: fetchGroupAttributeDescriptors ) {
			if ( initializedLazyAttributeNames.contains( fetchGroupAttributeDescriptor.getName() ) ) {
				// Already initialized
				if ( fetchGroupAttributeDescriptor.getName().equals( fieldName ) ) {
					resultStage = completedFuture( entry.getLoadedValue( fetchGroupAttributeDescriptor.getName() ) );
				}
				continue;
			}

			final Object selectedValue =  values[i++];
			if ( selectedValue instanceof CompletionStage ) {
				// This happens with a lazy one-to-one (bytecode enhancement enabled)
				CompletionStage<Object> selectedValueStage = (CompletionStage<Object>) selectedValue;
				resultStage = resultStage
						.thenCompose( result -> selectedValueStage
								.thenApply( selected -> {
									final boolean set = initializeLazyProperty(
											fieldName,
											entity,
											entry,
											fetchGroupAttributeDescriptor.getLazyIndex(),
											selected
									);
									if ( set ) {
										interceptor.attributeInitialized( fetchGroupAttributeDescriptor.getName() );
										return selected;
									}
									return result;
								} )
						);
			}
			else {
				final boolean set = initializeLazyProperty( fieldName, entity, entry, fetchGroupAttributeDescriptor.getLazyIndex(), selectedValue );
				if ( set ) {
					resultStage = completedFuture( selectedValue );
					interceptor.attributeInitialized( fetchGroupAttributeDescriptor.getName() );
				}
			}
		}

		return resultStage.thenApply( result -> {
			LOG.trace( "Done initializing lazy properties" );
			return result;
		} );
	}

	default CompletionStage<Object> reactiveInitializeEnhancedEntityUsedAsProxy(
			Object entity,
			String nameOfAttributeBeingAccessed,
			SharedSessionContractImplementor session) {

		final BytecodeEnhancementMetadata enhancementMetadata = getEntityMetamodel().getBytecodeEnhancementMetadata();
		final BytecodeLazyAttributeInterceptor currentInterceptor = enhancementMetadata.extractLazyInterceptor( entity );
		if ( currentInterceptor instanceof EnhancementAsProxyLazinessInterceptor) {
			final EnhancementAsProxyLazinessInterceptor proxyInterceptor =
					(EnhancementAsProxyLazinessInterceptor) currentInterceptor;

			final EntityKey entityKey = proxyInterceptor.getEntityKey();
			final Object identifier = entityKey.getIdentifier();

			return loadFromDatabaseOrCache( entity, session, entityKey, identifier )
					.thenApply( loaded -> {
						if ( loaded == null ) {
							final PersistenceContext persistenceContext = session.getPersistenceContext();
							persistenceContext.removeEntry( entity );
							persistenceContext.removeEntity( entityKey );
							session.getFactory().getEntityNotFoundDelegate()
									.handleEntityNotFound( entityKey.getEntityName(), identifier );
						}

						if ( nameOfAttributeBeingAccessed == null ) {
							return null;
						}
						else {
							final LazyAttributeLoadingInterceptor interceptor = enhancementMetadata
									.injectInterceptor( entity, identifier, session );
							return interceptor.readObject(
									entity,
									nameOfAttributeBeingAccessed,
									interceptor.isAttributeLoaded( nameOfAttributeBeingAccessed )
											? getPropertyValue( entity, nameOfAttributeBeingAccessed )
											: ( (LazyPropertyInitializer) this )
													.initializeLazyProperty( nameOfAttributeBeingAccessed, entity, session )
							);
						}
					} );
		}

		throw new IllegalStateException();
	}

	private CompletionStage<?> loadFromDatabaseOrCache(
			Object entity,
			SharedSessionContractImplementor session,
			EntityKey entityKey,
			Object identifier) {

		// note that stateless sessions don't interact with second-level cache
		if ( session instanceof EventSource && canReadFromCache() ) {
			Object cached = CacheEntityLoaderHelper.INSTANCE.loadFromSecondLevelCache(
					new LoadEvent( identifier, entity, (EventSource) session, false ),
					this,
					entityKey
			);
			if ( cached != null ) {
				return completedFuture( cached );
			}
		}

		return getReactiveSingleIdEntityLoader().load(
				identifier,
				entity,
				LockOptions.NONE,
				session
		);
	}

	boolean initializeLazyProperty(String fieldName, Object entity, EntityEntry entry, int lazyIndex, Object selectedValue);

	Object initializeLazyProperty(String fieldName, Object entity, SharedSessionContractImplementor session);

	String[][] getLazyPropertyColumnAliases();

	SingleIdArrayLoadPlan getSQLLazySelectLoadPlan(String fetchGroup);

	boolean isBatchable();

	// END AbstractEntityPersister methods
}
