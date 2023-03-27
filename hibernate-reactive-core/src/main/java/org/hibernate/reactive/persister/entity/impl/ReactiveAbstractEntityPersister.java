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
import java.util.*;
import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.StaleObjectStateException;
import org.hibernate.bytecode.enhance.spi.LazyPropertyInitializer;
import org.hibernate.bytecode.enhance.spi.interceptor.*;
import org.hibernate.bytecode.spi.BytecodeEnhancementMetadata;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.internal.ManagedTypeHelper;
import org.hibernate.engine.spi.*;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.jdbc.Expectation;
import org.hibernate.loader.ast.internal.CacheEntityLoaderHelper;
import org.hibernate.loader.ast.internal.LoaderSelectBuilder;
import org.hibernate.loader.ast.spi.NaturalIdLoader;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.metamodel.mapping.*;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationProcess;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.loader.ast.internal.ReactiveSingleIdArrayLoadPlan;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleIdEntityLoader;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.metamodel.mapping.internal.ReactiveCompoundNaturalIdMapping;
import org.hibernate.reactive.metamodel.mapping.internal.ReactiveSimpleNaturalIdMapping;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.session.impl.ReactiveQueryExecutorLookup;
import org.hibernate.sql.SimpleSelect;
import org.hibernate.sql.Update;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.type.BasicType;

import jakarta.persistence.metamodel.Attribute;

import static java.util.Collections.emptyMap;
import static org.hibernate.internal.util.collections.CollectionHelper.setOfSize;
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
		final SimpleSelect select = new SimpleSelect( factory )
				.setLockOptions( lockOptions )
				.setTableName( getRootTableName() )
				.addColumn( getRootTableIdentifierColumnNames()[0] )
				.addRestriction( getRootTableIdentifierColumnNames() );
		if ( isVersioned() ) {
			select.addRestriction( getVersionColumnName() );
		}
		if ( factory.getSessionFactoryOptions().isCommentsEnabled() ) {
			select.setComment( lockOptions.getLockMode() + " lock " + getEntityName() );
		}
		return parameters().process( select.toStatementString() );
	}

	default String generateUpdateLockString(LockOptions lockOptions) {
		final SessionFactoryImplementor factory = getFactory();
		final Update update = new Update( factory );
		update.setTableName( getRootTableName() );
		update.addAssignment( getVersionColumnName() );
		update.addRestriction( getRootTableIdentifierColumnNames() );
		update.addRestriction( getVersionColumnName() );
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

		final String fetchGroup = getEntityMetamodel().getBytecodeEnhancementMetadata()
				.getLazyAttributesMetadata()
				.getFetchGroupName( fieldName );
		final List<LazyAttributeDescriptor> fetchGroupAttributeDescriptors = getEntityMetamodel().getBytecodeEnhancementMetadata()
				.getLazyAttributesMetadata()
				.getFetchGroupAttributeDescriptors( fetchGroup );

		@SuppressWarnings("deprecation")
		Set<String> initializedLazyAttributeNames = interceptor.getInitializedLazyAttributeNames();

		// FIXME: How do I pass this to the query?
		Object[] arguments = PreparedStatementAdaptor.bind(
				statement -> getIdentifierType().nullSafeSet( statement, id, 1, session )
		);

		return reactiveGetSQLLazySelectLoadPlan( fetchGroup )
				.load( id, session )
				.thenCompose( values -> initLazyProperty(
						fieldName, entity,
						session, entry,
						interceptor,
						fetchGroupAttributeDescriptors,
						initializedLazyAttributeNames,
						values
				) );
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

	ReactiveSingleIdArrayLoadPlan reactiveGetSQLLazySelectLoadPlan(String fetchGroup);

	boolean isBatchable();

	/**
	 * @see AbstractEntityPersister#generateNaturalIdMapping(MappingModelCreationProcess, PersistentClass)
	 */
	default NaturalIdMapping generateNaturalIdMapping(
			MappingModelCreationProcess creationProcess,
			PersistentClass bootEntityDescriptor) {
		//noinspection AssertWithSideEffects
		assert bootEntityDescriptor.hasNaturalId();

		final int[] naturalIdAttributeIndexes = getEntityMetamodel().getNaturalIdentifierProperties();
		assert naturalIdAttributeIndexes.length > 0;

		if ( naturalIdAttributeIndexes.length == 1 ) {
			final String propertyName = getEntityMetamodel().getPropertyNames()[naturalIdAttributeIndexes[0]];
			final AttributeMapping attributeMapping = findAttributeMapping( propertyName );
			final SingularAttributeMapping singularAttributeMapping = (SingularAttributeMapping) attributeMapping;
			return new ReactiveSimpleNaturalIdMapping( singularAttributeMapping, this, creationProcess );
		}

		// collect the names of the attributes making up the natural-id.
		final Set<String> attributeNames = setOfSize( naturalIdAttributeIndexes.length );
		for ( int naturalIdAttributeIndex : naturalIdAttributeIndexes ) {
			attributeNames.add( this.getPropertyNames()[naturalIdAttributeIndex] );
		}

		// then iterate over the attribute mappings finding the ones having names
		// in the collected names.  iterate here because it is already alphabetical

		final List<SingularAttributeMapping> collectedAttrMappings = new ArrayList<>();
		for ( AttributeMapping attributeMapping : getAttributeMappings() ) {
			if ( attributeNames.contains( attributeMapping.getAttributeName() ) ) {
				collectedAttrMappings.add( (SingularAttributeMapping) attributeMapping );
			}
		}

		if ( collectedAttrMappings.size() <= 1 ) {
			throw new MappingException( "Expected multiple natural-id attributes, but found only one: " + getEntityName() );
		}

		return new ReactiveCompoundNaturalIdMapping( this, collectedAttrMappings, creationProcess );
	}

	@Override
	default NaturalIdLoader<?> getNaturalIdLoader() {
		return getNaturalIdMapping().makeLoader( this );
	}

	/**
	 * @see AbstractEntityPersister#getLazyLoadPlanByFetchGroup()
	 */
	default Map<String, ReactiveSingleIdArrayLoadPlan> getLazyLoadPlanByFetchGroup(String[] subclassPropertyNameClosure ) {
		final BytecodeEnhancementMetadata metadata = delegate().getEntityMetamodel().getBytecodeEnhancementMetadata();
		return metadata.isEnhancedForLazyLoading() && metadata.getLazyAttributesMetadata().hasLazyAttributes()
				? createLazyLoadPlanByFetchGroup( metadata, subclassPropertyNameClosure )
				: emptyMap();
	}

	default Map<String, ReactiveSingleIdArrayLoadPlan> createLazyLoadPlanByFetchGroup(BytecodeEnhancementMetadata metadata,  String[] subclassPropertyNameClosure ) {
		final Map<String, ReactiveSingleIdArrayLoadPlan> result = new HashMap<>();
		final LazyAttributesMetadata attributesMetadata = metadata.getLazyAttributesMetadata();
		for ( String groupName : attributesMetadata.getFetchGroupNames() ) {
			final ReactiveSingleIdArrayLoadPlan loadPlan =
					createLazyLoadPlan( attributesMetadata.getFetchGroupAttributeDescriptors( groupName), subclassPropertyNameClosure );
			if ( loadPlan != null ) {
				result.put( groupName, loadPlan );
			}
		}
		return result;
	}

	default ReactiveSingleIdArrayLoadPlan createLazyLoadPlan(List<LazyAttributeDescriptor> fetchGroupAttributeDescriptors, String[] subclassPropertyNameClosure ) {
		final List<ModelPart> partsToSelect = new ArrayList<>( fetchGroupAttributeDescriptors.size() );
		for ( LazyAttributeDescriptor lazyAttributeDescriptor : fetchGroupAttributeDescriptors ) {
			// all this only really needs to consider properties
			// of this class, not its subclasses, but since we
			// are reusing code used for sequential selects, we
			// use the subclass closure
			partsToSelect.add( getAttributeMapping( getSubclassPropertyIndex( lazyAttributeDescriptor.getName(), subclassPropertyNameClosure ) ) );
		}

		if ( partsToSelect.isEmpty() ) {
			// only one-to-one is lazily fetched
			return null;
		}
		else {
			final SessionFactoryImplementor factory = getFactory();
			final List<JdbcParameter> jdbcParameters = new ArrayList<>();
			final SelectStatement select = LoaderSelectBuilder.createSelect(
					this,
					partsToSelect,
					getIdentifierMapping(),
					null,
					1,
					LoadQueryInfluencers.NONE,
					LockOptions.NONE,
					jdbcParameters::add,
					factory
			);
			return new ReactiveSingleIdArrayLoadPlan( getIdentifierMapping(), select, jdbcParameters, LockOptions.NONE, factory );
		}
	}

	default int getSubclassPropertyIndex(String propertyName,  String[] subclassPropertyNameClosure ) {
		return ArrayHelper.indexOf( subclassPropertyNameClosure, propertyName );
	}
}
