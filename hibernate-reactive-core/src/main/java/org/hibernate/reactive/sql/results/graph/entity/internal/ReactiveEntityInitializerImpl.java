/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.hibernate.Hibernate;
import org.hibernate.LockMode;
import org.hibernate.annotations.NotFoundAction;
import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityHolder;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.EntityUniqueKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.PersistentAttributeInterceptor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.loader.ast.internal.CacheEntityLoaderHelper;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.proxy.map.MapProxy;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.results.graph.ReactiveInitializer;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.InitializerData;
import org.hibernate.sql.results.graph.InitializerParent;
import org.hibernate.sql.results.graph.entity.EntityResultGraphNode;
import org.hibernate.sql.results.graph.entity.internal.EntityInitializerImpl;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.type.Type;

import static org.hibernate.bytecode.enhance.spi.LazyPropertyInitializer.UNFETCHED_PROPERTY;
import static org.hibernate.engine.internal.ManagedTypeHelper.asPersistentAttributeInterceptable;
import static org.hibernate.engine.internal.ManagedTypeHelper.isPersistentAttributeInterceptable;
import static org.hibernate.metamodel.mapping.ForeignKeyDescriptor.Nature.TARGET;
import static org.hibernate.proxy.HibernateProxy.extractLazyInitializer;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.falseFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.trueFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveEntityInitializerImpl extends EntityInitializerImpl
		implements ReactiveInitializer<EntityInitializerImpl.EntityInitializerData> {

	public static class ReactiveEntityInitializerData extends EntityInitializerData {

		public ReactiveEntityInitializerData(RowProcessingState rowProcessingState) {
			super( rowProcessingState );
		}

		public void setEntityInstanceForNotify(Object instance) {
			super.entityInstanceForNotify = instance;
		}

		public Object getEntityInstanceForNotify() {
			return super.entityInstanceForNotify;
		}

		public EntityPersister getConcreteDescriptor() {
			return super.concreteDescriptor;
		}

		public void setConcreteDescriptor(EntityPersister entityPersister) {
			super.concreteDescriptor = entityPersister;
		}

		public EntityHolder getEntityHolder() {
			return super.entityHolder;
		}

		public void setEntityHolder(EntityHolder entityHolder) {
			super.entityHolder = entityHolder;
		}

		public EntityKey getEntityKey() {
			return super.entityKey;
		}

		public void setEntityKey(EntityKey entityKey) {
			super.entityKey = entityKey;
		}

		public String getUniqueKeyAttributePath() {
			return super.uniqueKeyAttributePath;
		}

		public void setUniqueKeyAttributePath(String uniqueKeyAttributePath) {
			super.uniqueKeyAttributePath = uniqueKeyAttributePath;
		}

		public Type[] getUniqueKeyPropertyTypes() {
			return super.uniqueKeyPropertyTypes;
		}

		public boolean getShallowCached() {
			return super.shallowCached;
		}

		public void setShallowCached(boolean shallowCached) {
			super.shallowCached = shallowCached;
		}

		public LockMode getLockMode() {
			return super.lockMode;
		}

		public void setLockMode(LockMode lockMode) {
			super.lockMode = lockMode;
		}
	}

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveEntityInitializerImpl(
			EntityResultGraphNode resultDescriptor,
			String sourceAlias,
			Fetch identifierFetch,
			Fetch discriminatorFetch,
			DomainResult<?> keyResult,
			DomainResult<Object> rowIdResult,
			NotFoundAction notFoundAction,
			boolean affectedByFilter,
			InitializerParent<?> parent,
			boolean isResultInitializer,
			AssemblerCreationState creationState) {
		super(
				resultDescriptor,
				sourceAlias,
				identifierFetch ,
				discriminatorFetch,
				keyResult,
				rowIdResult,
				notFoundAction,
				affectedByFilter,
				parent,
				isResultInitializer,
				creationState
		);
	}

	@Override
	protected void resolveEntityKey(EntityInitializerData original, Object id) {
		ReactiveEntityInitializerData data = (ReactiveEntityInitializerData) original;
		if ( data.getConcreteDescriptor() == null ) {
			data.setConcreteDescriptor( determineConcreteEntityDescriptor(
					data.getRowProcessingState(),
					getDiscriminatorAssembler(),
					getEntityDescriptor()
			) );
			assert data.getConcreteDescriptor() != null;
		}
		data.setEntityKey( new EntityKey( id, data.getConcreteDescriptor() ) );
	}

	@Override
	public CompletionStage<Void> reactiveResolveInstance(Object instance, EntityInitializerData original) {
		ReactiveEntityInitializerData data = (ReactiveEntityInitializerData) original;
		if ( instance == null ) {
			setMissing( data );
			return voidFuture();
		}
		data.setInstance( instance );
		final LazyInitializer lazyInitializer = extractLazyInitializer( data.getInstance() );
		final RowProcessingState rowProcessingState = data.getRowProcessingState();
		final SharedSessionContractImplementor session = rowProcessingState.getSession();
		if ( lazyInitializer == null ) {
			// Entity is most probably initialized
			data.setEntityInstanceForNotify( data.getInstance() );
			data.setConcreteDescriptor( session.getEntityPersister( null, data.getInstance() ) );
			resolveEntityKey( data, data.getConcreteDescriptor().getIdentifier( data.getInstance(), session ) );
			data.setEntityHolder( session.getPersistenceContextInternal().getEntityHolder( data.getEntityKey() ) );
			if ( data.getEntityHolder() == null ) {
				// Entity was most probably removed in the same session without setting the reference to null
				return reactiveResolveKey( data )
						.thenRun( () -> {
							assert data.getState() == State.MISSING;
							assert getInitializedPart() instanceof ToOneAttributeMapping
									&& ( (ToOneAttributeMapping) getInitializedPart() ).getSideNature() == TARGET;
						} );
			}
			// If the entity initializer is null, we know the entity is fully initialized,
			// otherwise it will be initialized by some other initializer
			data.setState( data.getEntityHolder().getEntityInitializer() == null ? State.INITIALIZED : State.RESOLVED );
		}
		else if ( lazyInitializer.isUninitialized() ) {
			data.setState( State.RESOLVED );
			// Read the discriminator from the result set if necessary
			EntityPersister persister = getDiscriminatorAssembler() == null
					? getEntityDescriptor()
					: determineConcreteEntityDescriptor( rowProcessingState, getDiscriminatorAssembler(), getEntityDescriptor() );
			data.setConcreteDescriptor( persister );
			assert data.getConcreteDescriptor() != null;
			resolveEntityKey( data, lazyInitializer.getIdentifier() );
			data.setEntityHolder( session.getPersistenceContextInternal().claimEntityHolderIfPossible(
					data.getEntityKey(),
					null,
					rowProcessingState.getJdbcValuesSourceProcessingState(),
					this
			) );
			// Resolve and potentially create the entity instance
			data.setEntityInstanceForNotify( resolveEntityInstance( data ) );
			lazyInitializer.setImplementation( data.getEntityInstanceForNotify() );
			registerLoadingEntity( data, data.getEntityInstanceForNotify() );
		}
		else {
			data.setState( State.INITIALIZED );
			data.setEntityInstanceForNotify( lazyInitializer.getImplementation() );
			data.setConcreteDescriptor( session.getEntityPersister( null, data.getEntityInstanceForNotify() ) );
			resolveEntityKey( data, lazyInitializer.getIdentifier() );
			data.setEntityHolder( session.getPersistenceContextInternal().getEntityHolder( data.getEntityKey() ) );
		}
		return reactiveInitializeStage( data, rowProcessingState )
				.thenCompose( v -> {
					upgradeLockMode( data );
					if ( data.getState() == State.INITIALIZED ) {
						registerReloadedEntity( data );
						resolveInstanceSubInitializers( data );
						if ( rowProcessingState.needsResolveState() ) {
							// We need to read result set values to correctly populate the query cache
							resolveState( data );
						}
						return voidFuture();
					}
					else {
						return reactiveResolveKeySubInitializers( data );
					}
				} );
	}

	private CompletionStage<Void> reactiveInitializeStage(
			ReactiveEntityInitializerData data,
			RowProcessingState rowProcessingState) {
		if ( getIdentifierAssembler() != null ) {
			final Initializer<?> initializer = getIdentifierAssembler().getInitializer();
			if ( initializer != null ) {
				if ( initializer instanceof ReactiveInitializer ) {
					return ( (ReactiveInitializer<?>) initializer )
							.reactiveResolveInstance( data.getEntityKey().getIdentifier(), rowProcessingState );
				}
				else {
					initializer.resolveInstance( data.getEntityKey().getIdentifier(), rowProcessingState );
				}
			}
		}
		return voidFuture();
	}

	@Override
	public CompletionStage<Void> reactiveResolveInstance(EntityInitializerData original) {
		ReactiveEntityInitializerData data = (ReactiveEntityInitializerData) original;
		if ( data.getState() != State.KEY_RESOLVED ) {
			return voidFuture();
		}
		final RowProcessingState rowProcessingState = data.getRowProcessingState();
		data.setState( State.RESOLVED );
		if ( data.getEntityKey() == null ) {
			assert getIdentifierAssembler() != null;
 			final Object id = getIdentifierAssembler().assemble( rowProcessingState );
			if ( id == null ) {
				setMissing( data );
				return voidFuture();
			}
			resolveEntityKey( data, id );
		}
		final PersistenceContext persistenceContext = rowProcessingState.getSession()
				.getPersistenceContextInternal();
		data.setEntityHolder( persistenceContext.claimEntityHolderIfPossible(
				data.getEntityKey(),
				null,
				rowProcessingState.getJdbcValuesSourceProcessingState(),
				this
		) );

		if ( useEmbeddedIdentifierInstanceAsEntity( data ) ) {
			data.setEntityInstanceForNotify( rowProcessingState.getEntityId() );
			data.setInstance( data.getEntityInstanceForNotify() );
		}
		else {
			return reactiveResolveEntityInstance1( data )
					.thenAccept( v -> {
						if ( data.getUniqueKeyAttributePath() != null ) {
							final SharedSessionContractImplementor session = rowProcessingState.getSession();
							final EntityPersister concreteDescriptor = getConcreteDescriptor( data );
							final EntityUniqueKey euk = new EntityUniqueKey(
									concreteDescriptor.getEntityName(),
									data.getUniqueKeyAttributePath(),
									rowProcessingState.getEntityUniqueKey(),
									data.getUniqueKeyPropertyTypes()[concreteDescriptor.getSubclassId()],
									session.getFactory()
							);
							session.getPersistenceContextInternal().addEntity( euk, getEntityInstance( data ) );
						}
						postResolveInstance( data );
					} );
		}
		postResolveInstance( data );
		return voidFuture();
	}

	private void postResolveInstance(ReactiveEntityInitializerData data) {
		if ( data.getInstance() != null ) {
			upgradeLockMode( data );
			if ( data.getState() == State.INITIALIZED ) {
				registerReloadedEntity( data );
				if ( data.getRowProcessingState().needsResolveState() ) {
					// We need to read result set values to correctly populate the query cache
					resolveState( data );
				}
			}
			if ( data.getShallowCached() ) {
				initializeSubInstancesFromParent( data );
			}
		}
	}

	@Override
	public CompletionStage<Void> reactiveInitializeInstance(EntityInitializerData data) {
		if ( data.getState() != State.RESOLVED ) {
			return voidFuture();
		}
		if ( !skipInitialization( data ) ) {
			assert consistentInstance( data );
			return reactiveInitializeEntityInstance( (ReactiveEntityInitializerData) data );
		}
		data.setState( State.INITIALIZED );
		return voidFuture();
	}

	protected CompletionStage<Void> reactiveInitializeEntityInstance(ReactiveEntityInitializerData data) {
		final RowProcessingState rowProcessingState = data.getRowProcessingState();
		final Object entityIdentifier = data.getEntityKey().getIdentifier();
		final SharedSessionContractImplementor session = rowProcessingState.getSession();
		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();

		return reactiveExtractConcreteTypeStateValues( data )
				.thenAccept( resolvedEntityState -> {

					preLoad( data, resolvedEntityState );

					if ( isPersistentAttributeInterceptable( data.getEntityInstanceForNotify() ) ) {
						final PersistentAttributeInterceptor persistentAttributeInterceptor =
								asPersistentAttributeInterceptable( data.getEntityInstanceForNotify() ).$$_hibernate_getInterceptor();
						if ( persistentAttributeInterceptor == null
								|| persistentAttributeInterceptor instanceof EnhancementAsProxyLazinessInterceptor ) {
							// if we do this after the entity has been initialized the
							// BytecodeLazyAttributeInterceptor#isAttributeLoaded(String fieldName) would return false;
							data.getConcreteDescriptor().getBytecodeEnhancementMetadata()
									.injectInterceptor( data.getEntityInstanceForNotify(), entityIdentifier, session );
						}
					}
					data.getConcreteDescriptor().setPropertyValues( data.getEntityInstanceForNotify(), resolvedEntityState );

					persistenceContext.addEntity( data.getEntityKey(), data.getEntityInstanceForNotify() );

					// Also register possible unique key entries
					registerPossibleUniqueKeyEntries( data, resolvedEntityState, session );

					final Object version = getVersionAssembler() != null ? getVersionAssembler().assemble( rowProcessingState ) : null;
					final Object rowId = getRowIdAssembler() != null ? getRowIdAssembler().assemble( rowProcessingState ) : null;

					// from the perspective of Hibernate, an entity is read locked as soon as it is read
					// so regardless of the requested lock mode, we upgrade to at least the read level
					final LockMode lockModeToAcquire = data.getLockMode() == LockMode.NONE ? LockMode.READ : data.getLockMode();

					final EntityEntry entityEntry = persistenceContext.addEntry(
							data.getEntityInstanceForNotify(),
							Status.LOADING,
							resolvedEntityState,
							rowId,
							data.getEntityKey().getIdentifier(),
							version,
							lockModeToAcquire,
							true,
							data.getConcreteDescriptor(),
							false
					);
					data.getEntityHolder().setEntityEntry( entityEntry );

					registerNaturalIdResolution( data, persistenceContext, resolvedEntityState );

					takeSnapshot( data, session, persistenceContext, entityEntry, resolvedEntityState );

					data.getConcreteDescriptor().afterInitialize( data.getEntityInstanceForNotify(), session );

					assert data.getConcreteDescriptor().getIdentifier( data.getEntityInstanceForNotify(), session ) != null;

					final StatisticsImplementor statistics = session.getFactory().getStatistics();
					if ( statistics.isStatisticsEnabled() ) {
						if ( !rowProcessingState.isQueryCacheHit() ) {
							statistics.loadEntity( data.getConcreteDescriptor().getEntityName() );
						}
					}
					updateCaches(
							data,
							session,
							session.getPersistenceContextInternal(),
							resolvedEntityState,
							version
					);
				} );
	}

	protected CompletionStage<Object[]> reactiveExtractConcreteTypeStateValues(ReactiveEntityInitializerData data) {
		final RowProcessingState rowProcessingState = data.getRowProcessingState();
		final Object[] values = new Object[data.getConcreteDescriptor().getNumberOfAttributeMappings()];
		final DomainResultAssembler<?>[] concreteAssemblers = getAssemblers()[data.getConcreteDescriptor().getSubclassId()];
		return loop( 0, values.length, i -> {
			final DomainResultAssembler<?> assembler = concreteAssemblers[i];
			if ( assembler instanceof ReactiveEntityAssembler ) {
				return ( (ReactiveEntityAssembler) assembler )
						.reactiveAssemble( (ReactiveRowProcessingState) rowProcessingState )
						.thenAccept( assembled -> values[i] = assembled );
			}
			values[i] = assembler == null ? UNFETCHED_PROPERTY : assembler.assemble( rowProcessingState );
			return voidFuture();
		} ).thenApply( v -> values );
	}

	protected CompletionStage<Void> reactiveResolveEntityInstance1(ReactiveEntityInitializerData data) {
		final Object proxy = data.getEntityHolder().getProxy();
		final boolean unwrapProxy = proxy != null && getInitializedPart() instanceof ToOneAttributeMapping
				&& ( (ToOneAttributeMapping) getInitializedPart() ).isUnwrapProxy()
				&& getConcreteDescriptor( data ).getBytecodeEnhancementMetadata().isEnhancedForLazyLoading();
		final Object entityFromExecutionContext;
		if ( !unwrapProxy && isProxyInstance( proxy ) ) {
			if ( ( entityFromExecutionContext = getEntityFromExecutionContext( data ) ) != null ) {
				data.setEntityInstanceForNotify( entityFromExecutionContext );
				data.setInstance( data.getEntityInstanceForNotify() );
				// If the entity comes from the execution context, it is treated as not initialized
				// so that we can refresh the data as requested
				registerReloadedEntity( data );
			}
			else {
				data.setInstance( proxy );
				if ( Hibernate.isInitialized( data.getInstance() ) ) {
					data.setState( State.INITIALIZED );
					data.setEntityInstanceForNotify( Hibernate.unproxy( data.getInstance() ) );
				}
				else {
					final LazyInitializer lazyInitializer = extractLazyInitializer( data.getInstance() );
					assert lazyInitializer != null;
					return reactiveResolveEntityInstance2( data )
							.thenAccept( entityInstance -> {
								data.setEntityInstanceForNotify( entityInstance );
								lazyInitializer.setImplementation( data.getEntityInstanceForNotify() );
								ensureEntityIsInitialized( data );
							} );
				}
			}
		}
		else {
			final Object existingEntity = data.getEntityHolder().getEntity();
			if ( existingEntity != null ) {
				data.setEntityInstanceForNotify( existingEntity );
				data.setInstance( data.getEntityInstanceForNotify() );
				if ( data.getEntityHolder().getEntityInitializer() == null ) {
					assert data.getEntityHolder().isInitialized() == isExistingEntityInitialized( existingEntity );
					if ( data.getEntityHolder().isInitialized() ) {
						data.setState( State.INITIALIZED );
					}
					else if ( isResultInitializer() ) {
						registerLoadingEntity( data, data.getInstance() );
					}
				}
				else if ( data.getEntityHolder().getEntityInitializer() != this ) {
					data.setState( State.INITIALIZED );
				}
			}
			else if ( ( entityFromExecutionContext = getEntityFromExecutionContext( data ) ) != null ) {
				// This is the entity to refresh, so don't set the state to initialized
				data.setEntityInstanceForNotify( entityFromExecutionContext );
				data.setInstance( data.getEntityInstanceForNotify() );
				if ( isResultInitializer() ) {
					registerLoadingEntity( data, data.getInstance() );
				}
			}
			else {
				assert data.getEntityHolder().getEntityInitializer() == this;
				// look to see if another initializer from a parent load context or an earlier
				// initializer is already loading the entity
				return reactiveResolveEntityInstance2( data )
						.thenAccept( entityInstance -> {
							data.setEntityInstanceForNotify( entityInstance );
							data.setInstance( data.getEntityInstanceForNotify() );
							final Initializer<?> idInitializer;
							if ( data.getEntityHolder().getEntityInitializer() == this && data.getState() != State.INITIALIZED
									&& getIdentifierAssembler() != null
									&& ( idInitializer = getIdentifierAssembler().getInitializer() ) != null ) {
								// If this is the owning initializer and the returned object is not initialized,
								// this means that the entity instance was just instantiated.
								// In this case, we want to call "assemble" and hence "initializeInstance" on the initializer
								// for possibly non-aggregated identifier mappings, so inject the virtual id representation
								idInitializer.initializeInstance( data.getRowProcessingState() );
							}
							ensureEntityIsInitialized( data );
						} );
			}
		}
		ensureEntityIsInitialized( data );
		return voidFuture();
	}

	private void ensureEntityIsInitialized(ReactiveEntityInitializerData data) {
		// todo: ensure we initialize the entity
		assert !data.getShallowCached() || data.getState() == State.INITIALIZED : "Forgot to initialize the entity";
	}

	protected CompletionStage<Object> reactiveResolveEntityInstance2(ReactiveEntityInitializerData data) {
		if ( data.getEntityHolder().getEntityInitializer() == this ) {
			assert data.getEntityHolder().getEntity() == null;
			return reactiveResolveEntityInstance( data );
		}
		else {
			// the entity is already being loaded elsewhere
			return completedFuture( data.getEntityHolder().getEntity() );
		}
	}

	protected CompletionStage<Object> reactiveResolveEntityInstance(ReactiveEntityInitializerData data) {
		final RowProcessingState rowProcessingState = data.getRowProcessingState();
		final Object resolved = resolveToOptionalInstance( data );
		if ( resolved != null ) {
			registerLoadingEntity( data, resolved );
			return completedFuture( resolved );
		}
		else {
			if ( rowProcessingState.isQueryCacheHit() && getEntityDescriptor().useShallowQueryCacheLayout() ) {
				// We must load the entity this way, because the query cache entry contains only the primary key
				data.setState( State.INITIALIZED );
				final SharedSessionContractImplementor session = rowProcessingState.getSession();
				assert data.getEntityHolder().getEntityInitializer() == this;
				// If this initializer owns the entity, we have to remove the entity holder,
				// because the subsequent loading process will claim the entity
				session.getPersistenceContextInternal().removeEntityHolder( data.getEntityKey() );
				return ( (ReactiveSession) session ).reactiveInternalLoad(
						data.getConcreteDescriptor().getEntityName(),
						data.getEntityKey().getIdentifier(),
						true,
						false
				);
			}
			// We have to query the second level cache if reference cache entries are used
			else if ( getEntityDescriptor().canUseReferenceCacheEntries() ) {
				final Object cached = resolveInstanceFromCache( data );
				if ( cached != null ) {
					// EARLY EXIT!!!
					// because the second level cache has reference cache entries, the entity is initialized
					data.setState( State.INITIALIZED );
					return completedFuture( cached );
				}
			}
			final Object instance = instantiateEntity( data );
			registerLoadingEntity( data, instance );
			return completedFuture( instance );
		}
	}

	// FIXME: I could change the scope of this method in ORM
	private Object resolveToOptionalInstance(ReactiveEntityInitializerData data) {
		if ( isResultInitializer() ) {
			// this isEntityReturn bit is just for entity loaders, not hql/criteria
			final JdbcValuesSourceProcessingOptions processingOptions =
					data.getRowProcessingState().getJdbcValuesSourceProcessingState().getProcessingOptions();
			return matchesOptionalInstance( data, processingOptions ) ? processingOptions.getEffectiveOptionalObject() : null;
		}
		else {
			return null;
		}
	}

	// FIXME: I could change the scope of this method in ORM
	private boolean isProxyInstance(Object proxy) {
		return proxy != null
				&& ( proxy instanceof MapProxy || getEntityDescriptor().getJavaType().getJavaTypeClass().isInstance( proxy ) );
	}

	// FIXME: I could change the scope of this method in ORM
	private Object resolveInstanceFromCache(ReactiveEntityInitializerData data) {
		return CacheEntityLoaderHelper.INSTANCE.loadFromSecondLevelCache(
				data.getRowProcessingState().getSession().asEventSource(),
				null,
				data.getLockMode(),
				getEntityDescriptor(),
				data.getEntityKey()
		);
	}

	// FIXME: I could change the scope of this method in ORM
	private boolean matchesOptionalInstance(
			ReactiveEntityInitializerData data,
			JdbcValuesSourceProcessingOptions processingOptions) {
		final Object optionalEntityInstance = processingOptions.getEffectiveOptionalObject();
		final Object requestedEntityId = processingOptions.getEffectiveOptionalId();
		return requestedEntityId != null
				&& optionalEntityInstance != null
				&& requestedEntityId.equals( data.getEntityKey().getIdentifier() );
	}

	private boolean isExistingEntityInitialized(Object existingEntity) {
		return Hibernate.isInitialized( existingEntity );
	}

	@Override
	public CompletionStage<Void> reactiveResolveKey(EntityInitializerData data) {
		return reactiveResolveKey( (ReactiveEntityInitializerData) data, false );
	}

	protected CompletionStage<Void> reactiveResolveKey(ReactiveEntityInitializerData data, boolean entityKeyOnly) {
		// todo (6.0) : atm we do not handle sequential selects
		// 		- see AbstractEntityPersister#hasSequentialSelect and
		//			AbstractEntityPersister#getSequentialSelect in 5.2
		if ( data.getState() != State.UNINITIALIZED ) {
			return voidFuture();
		}
		data.setState( State.KEY_RESOLVED );

		// reset row state
		data.setConcreteDescriptor( null );
		data.setEntityKey( null );
		data.setInstance( null );
		data.setEntityInstanceForNotify( null );
		data.setEntityHolder( null );

		final Object[] id = new Object[1];
		return initializeId( data, id, entityKeyOnly )
				.thenCompose( initialized -> {
					if ( initialized ) {
						resolveEntityKey( data, id[0] );
						if ( !entityKeyOnly ) {
							// Resolve the entity instance early as we have no key many-to-one
							return reactiveResolveInstance( data )
									.thenCompose( v -> {
										if ( !data.getShallowCached() ) {
											if ( data.getState() == State.INITIALIZED ) {
												if ( data.getEntityHolder().getEntityInitializer() == null ) {
													// The entity is already part of the persistence context,
													// so let's figure out the loaded state and only run sub-initializers if necessary
													return reactiveResolveInstanceSubInitializers( data );
												}
												// If the entity is initialized and getEntityInitializer() == this,
												// we already processed a row for this entity before,
												// but we still have to call resolveKeySubInitializers to activate sub-initializers,
												// because a row might contain data that sub-initializers want to consume
												else {
													// todo: try to diff the eagerness of the sub-initializers to avoid further processing
													return reactiveResolveKeySubInitializers( data );
												}
											}
											else {
												return reactiveResolveKeySubInitializers( data );
											}
										}
										return voidFuture();
									} );
						}
					}
					return voidFuture();
				} );
	}


	protected CompletionStage<Void> reactiveResolveInstanceSubInitializers(ReactiveEntityInitializerData data) {
		final Initializer<?>[] initializers = getSubInitializers()[data.getConcreteDescriptor().getSubclassId()];
		if ( initializers.length == 0 ) {
			return voidFuture();
		}
		final EntityEntry entityEntry = data.getEntityHolder().getEntityEntry();
		final RowProcessingState rowProcessingState = data.getRowProcessingState();
		assert entityEntry == rowProcessingState.getSession()
				.getPersistenceContextInternal()
				.getEntry( data.getEntityInstanceForNotify() );
		final Object[] loadedState = entityEntry.getLoadedState();
		final Object[] state;
		if ( loadedState == null ) {
			if ( entityEntry.getStatus() == Status.READ_ONLY ) {
				state = data.getConcreteDescriptor().getValues( data.getEntityInstanceForNotify() );
			}
			else {
				// This branch is entered when a load happens while a cache entry is assembling.
				// The EntityEntry has the LOADING state, but the loaded state is still empty.
				assert entityEntry.getStatus() == Status.LOADING;
				// Just skip any initialization in this case as the cache entry assembling will take care of it
				return voidFuture();
			}
		}
		else {
			state = loadedState;
		}
		return loop( 0, initializers.length, i -> {
			final Initializer<?> initializer = initializers[i];
			if ( initializer != null ) {
				final Object subInstance = state[i];
				if ( subInstance == UNFETCHED_PROPERTY ) {
					if ( initializer instanceof ReactiveInitializer ) {
						return ( (ReactiveInitializer<?>) initializer )
								.reactiveResolveKey( rowProcessingState );
					}
					else {
						// Go through the normal initializer process
						initializer.resolveKey( rowProcessingState );
					}
				}
				else {
					if ( initializer instanceof ReactiveInitializer ) {
						return ( (ReactiveInitializer<?>) initializer )
								.reactiveResolveInstance( subInstance, rowProcessingState );
					}
					else {
						initializer.resolveInstance( subInstance, rowProcessingState );
					}
				}
			}
			return voidFuture();
		} );
	}

	protected CompletionStage<Void> reactiveResolveKeySubInitializers(ReactiveEntityInitializerData data) {
		final RowProcessingState rowProcessingState = data.getRowProcessingState();
		return loop(
				getSubInitializers()[data.getConcreteDescriptor().getSubclassId()],
				initializer -> {
					if ( initializer != null ) {
						if ( initializer instanceof ReactiveInitializer ) {
							return ( (ReactiveInitializer<?>) initializer ).reactiveResolveKey( rowProcessingState );
						}
						initializer.resolveKey( rowProcessingState );
					}
					return voidFuture();
				}
		);
	}

	/**
	 * Return {@code true} if the identifier has been initialized
	 */
	private CompletionStage<Boolean> initializeId(ReactiveEntityInitializerData data, Object[] id, boolean entityKeyOnly) {
		final RowProcessingState rowProcessingState = data.getRowProcessingState();
		if ( getIdentifierAssembler() == null ) {
			id[0] = rowProcessingState.getEntityId();
			assert id[0] != null : "Initializer requires a not null id for loading";
			return trueFuture();
		}
		else {
			//noinspection unchecked
			final Initializer<InitializerData> initializer = (Initializer<InitializerData>) getIdentifierAssembler().getInitializer();
			if ( initializer != null ) {
				final InitializerData subData = initializer.getData( rowProcessingState );
				return ( (ReactiveInitializer<InitializerData>) initializer )
						.reactiveResolveKey( subData )
						.thenCompose( v -> {
							if ( subData.getState() == State.MISSING ) {
								setMissing( data );
								return falseFuture();
							}
							else {
								data.setConcreteDescriptor( determineConcreteEntityDescriptor(
										rowProcessingState,
										getDiscriminatorAssembler(),
										getEntityDescriptor()
								) );
								assert data.getConcreteDescriptor() != null;
								if ( isKeyManyToOne() ) {
									if ( !data.getShallowCached() && !entityKeyOnly ) {
										resolveKeySubInitializers( data );
									}
									return falseFuture();
								}
							}
							id[0] = getIdentifierAssembler().assemble( rowProcessingState );
							if ( id[0] == null ) {
								setMissing( data );
								return falseFuture();
							}
							return trueFuture();
						} );
			}
			id[0] = getIdentifierAssembler().assemble( rowProcessingState );
			if ( id[0] == null ) {
				setMissing( data );
				return falseFuture();
			}
			return trueFuture();
		}
	}

	@Override
	protected EntityInitializerData createInitializerData(RowProcessingState rowProcessingState) {
		return new ReactiveEntityInitializerData( rowProcessingState );
	}

	@Override
	public CompletionStage<Void> forEachReactiveSubInitializer(
			BiFunction<ReactiveInitializer<?>, RowProcessingState, CompletionStage<Void>> consumer,
			InitializerData data) {
		final RowProcessingState rowProcessingState = data.getRowProcessingState();
		return voidFuture()
				.thenCompose( v -> {
					if ( getKeyAssembler() != null ) {
						final Initializer<?> initializer = getKeyAssembler().getInitializer();
						if ( initializer != null ) {
							return consumer.apply( (ReactiveInitializer<?>) initializer, rowProcessingState );
						}
					}
					return voidFuture();
				} )
				.thenCompose( v -> {
					if ( getIdentifierAssembler() != null ) {
						final Initializer<?> initializer = getIdentifierAssembler().getInitializer();
						if ( initializer != null ) {
							consumer.apply( (ReactiveInitializer<?>) initializer, rowProcessingState );
						}
					}
					return voidFuture();
				} )
				.thenCompose( v -> {
					final ReactiveEntityInitializerDataAdaptor entityInitializerData = new ReactiveEntityInitializerDataAdaptor(
							(EntityInitializerData) data );
					if ( entityInitializerData.getConcreteDescriptor() == null ) {
						return loop( getSubInitializers(), initializers ->
						 	loop( initializers, initializer -> {
								if ( initializer != null ) {
									return consumer.apply( (ReactiveInitializer<?>) initializer, rowProcessingState );
								}
								return voidFuture();
							} )
						);
					}
					else {
						Initializer<?>[] subInitializers = getSubInitializers()[entityInitializerData.getConcreteDescriptor()
								.getSubclassId()];
						return loop( subInitializers, initializer -> consumer
								.apply( (ReactiveInitializer<?>) initializer, rowProcessingState )
						);
					}
				} );
	}

	private static class ReactiveEntityInitializerDataAdaptor extends EntityInitializerData {

		public ReactiveEntityInitializerDataAdaptor(EntityInitializerData delegate) {
			super( delegate );
		}

		public EntityPersister getConcreteDescriptor() {
			return concreteDescriptor;
		}
	}
}
