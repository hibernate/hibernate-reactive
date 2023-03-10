/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockMode;
import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.PersistentAttributeInterceptor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.results.graph.ReactiveDomainResultsAssembler;
import org.hibernate.reactive.sql.results.graph.ReactiveInitializer;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.Fetch;
import org.hibernate.sql.results.graph.entity.AbstractEntityInitializer;
import org.hibernate.sql.results.graph.entity.EntityLoadingLogging;
import org.hibernate.sql.results.graph.entity.EntityResultGraphNode;
import org.hibernate.sql.results.graph.entity.LoadingEntityEntry;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;
import org.hibernate.stat.spi.StatisticsImplementor;

import static org.hibernate.bytecode.enhance.spi.LazyPropertyInitializer.UNFETCHED_PROPERTY;
import static org.hibernate.engine.internal.ManagedTypeHelper.asPersistentAttributeInterceptable;
import static org.hibernate.engine.internal.ManagedTypeHelper.isPersistentAttributeInterceptable;
import static org.hibernate.internal.log.LoggingHelper.toLoggableString;
import static org.hibernate.proxy.HibernateProxy.extractLazyInitializer;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public abstract class ReactiveAbstractEntityInitializer extends AbstractEntityInitializer implements ReactiveInitializer {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	protected ReactiveAbstractEntityInitializer(
			EntityResultGraphNode resultDescriptor,
			NavigablePath navigablePath,
			LockMode lockMode,
			Fetch identifierFetch,
			Fetch discriminatorFetch,
			DomainResult<Object> rowIdResult,
			AssemblerCreationState creationState) {
		super(
				resultDescriptor,
				navigablePath,
				lockMode,
				identifierFetch,
				discriminatorFetch,
				rowIdResult,
				creationState
		);
	}

	@Override
	public void resolveInstance(RowProcessingState rowProcessingState) {
		super.resolveInstance( rowProcessingState );
	}

	@Override
	public void initializeInstance(RowProcessingState rowProcessingState) {
		throw LOG.nonReactiveMethodCall( "reactiveInitializeInstance" );
	}

	@Override
	public CompletionStage<Void> reactiveResolveInstance(ReactiveRowProcessingState rowProcessingState) {
		super.resolveInstance( rowProcessingState );
		return voidFuture();
	}

	@Override
	public CompletionStage<Void> reactiveInitializeInstance(ReactiveRowProcessingState rowProcessingState) {
		if ( isMissing() || isEntityInitialized() ) {
			return voidFuture();
		}

		preLoad( rowProcessingState );

		final LazyInitializer lazyInitializer = extractLazyInitializer( getEntityInstance() );
		return voidFuture()
				.thenCompose( v -> {
					if ( lazyInitializer != null ) {
						return lazyInitialize( rowProcessingState, lazyInitializer );
					}
					else {
						// FIXME: Read from cache if possible
						return initializeEntity( getEntityInstance(), rowProcessingState )
								.thenAccept( ignore -> setEntityInstanceForNotify( getEntityInstance() ) );
					}
				} )
				.thenAccept( o -> {
					notifyResolutionListeners( getEntityInstanceForNotify() );
					setEntityInitialized( true );
				} );
	}

	private CompletionStage<Void> lazyInitialize(
			ReactiveRowProcessingState rowProcessingState,
			LazyInitializer lazyInitializer) {
		final SharedSessionContractImplementor session = rowProcessingState.getSession();
		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		Object instance = persistenceContext.getEntity( getEntityKey() );
		if ( instance == null ) {
			return resolveInstance( rowProcessingState, lazyInitializer, persistenceContext );
		}
		lazyInitializer.setImplementation( instance );
		setEntityInstanceForNotify( instance );
		return voidFuture();
	}

	private CompletionStage<Void> resolveInstance(
			ReactiveRowProcessingState rowProcessingState,
			LazyInitializer lazyInitializer,
			PersistenceContext persistenceContext) {
		final Object instance = resolveInstance(
				getEntityKey().getIdentifier(),
				persistenceContext.getLoadContexts().findLoadingEntityEntry( getEntityKey() ),
				rowProcessingState
		);
		return initializeEntity( instance, rowProcessingState )
				.thenAccept( v -> {
					lazyInitializer.setImplementation( instance );
					setEntityInstanceForNotify( instance );
				} );
	}

	private Object resolveInstance(
			Object entityIdentifier,
			LoadingEntityEntry existingLoadingEntry,
			RowProcessingState rowProcessingState) {
		if ( isOwningInitializer() ) {
			assert existingLoadingEntry == null || existingLoadingEntry.getEntityInstance() == null;
			return resolveEntityInstance( entityIdentifier, rowProcessingState );
		}
		else {
			// the entity is already being loaded elsewhere
			if ( EntityLoadingLogging.DEBUG_ENABLED ) {
				EntityLoadingLogging.ENTITY_LOADING_LOGGER.debugf(
						"(%s) Entity [%s] being loaded by another initializer [%s] - skipping processing",
						getSimpleConcreteImplName(),
						toLoggableString( getNavigablePath(), entityIdentifier ),
						existingLoadingEntry.getEntityInitializer()
				);
			}
			return existingLoadingEntry.getEntityInstance();
		}
	}

	private CompletionStage<Void> initializeEntity(Object toInitialize, RowProcessingState rowProcessingState) {
		if ( !skipInitialization( toInitialize, rowProcessingState ) ) {
			assert consistentInstance( toInitialize, rowProcessingState );
			return initializeEntityInstance( toInitialize, rowProcessingState );
		}
		return voidFuture();
	}


	protected CompletionStage<Object[]> reactiveExtractConcreteTypeStateValues(RowProcessingState rowProcessingState) {
		final Object[] values = new Object[getConcreteDescriptor().getNumberOfAttributeMappings()];
		final DomainResultAssembler<?>[] concreteAssemblers = getAssemblers()[getConcreteDescriptor().getSubclassId()];
		return loop( 0, values.length, i -> {
			final DomainResultAssembler<?> assembler = concreteAssemblers[i];
			if ( assembler instanceof  ReactiveDomainResultsAssembler) {
				return ( (ReactiveDomainResultsAssembler) assembler )
						.reactiveAssemble( (ReactiveRowProcessingState) rowProcessingState )
						.thenAccept( obj -> values[i] = obj );
			}
			else {
				values[i] = assembler == null ? UNFETCHED_PROPERTY : assembler.assemble( rowProcessingState );
				return voidFuture();
			}
		} ).thenApply( unused -> values );
	}

	private CompletionStage<Void> initializeEntityInstance(Object toInitialize, RowProcessingState rowProcessingState) {
		final Object entityIdentifier = getEntityKey().getIdentifier();
		final SharedSessionContractImplementor session = rowProcessingState.getSession();
		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();

		if ( EntityLoadingLogging.TRACE_ENABLED ) {
			EntityLoadingLogging.ENTITY_LOADING_LOGGER.tracef(
					"(%s) Beginning Initializer#initializeInstance process for entity %s",
					getSimpleConcreteImplName(),
					toLoggableString( getNavigablePath(), entityIdentifier )
			);
		}

		getEntityDescriptor().setIdentifier( toInitialize, entityIdentifier, session );
		return reactiveExtractConcreteTypeStateValues( rowProcessingState )
				.thenCompose( entityState -> loop( 0, entityState.length, i -> {
								  if ( entityState[i] instanceof CompletionStage ) {
									  CompletionStage<Object> stateStage = (CompletionStage<Object>) entityState[i];
									  return stateStage.thenAccept( state -> entityState[i] = state );
								  }
								  return voidFuture();
							  } ).thenAccept( v -> setResolvedEntityState( entityState ) )
				)
				.thenAccept( v -> {
					if ( isPersistentAttributeInterceptable(toInitialize) ) {
						PersistentAttributeInterceptor persistentAttributeInterceptor =
								asPersistentAttributeInterceptable( toInitialize ).$$_hibernate_getInterceptor();
						if ( persistentAttributeInterceptor == null
								|| persistentAttributeInterceptor instanceof EnhancementAsProxyLazinessInterceptor ) {
							// if we do this after the entity has been initialized the
							// BytecodeLazyAttributeInterceptor#isAttributeLoaded(String fieldName) would return false;
							getConcreteDescriptor().getBytecodeEnhancementMetadata()
									.injectInterceptor( toInitialize, entityIdentifier, session );
						}
					}
					getConcreteDescriptor().setPropertyValues( toInitialize, getResolvedEntityState() );
					persistenceContext.addEntity( getEntityKey(), toInitialize );

					// Also register possible unique key entries
					registerPossibleUniqueKeyEntries( toInitialize, session );

					final Object version = getVersionAssembler() != null ? getVersionAssembler().assemble( rowProcessingState ) : null;
					final Object rowId = getRowIdAssembler() != null ? getRowIdAssembler().assemble( rowProcessingState ) : null;

					// from the perspective of Hibernate, an entity is read locked as soon as it is read
					// so regardless of the requested lock mode, we upgrade to at least the read level
					final LockMode lockModeToAcquire = getLockMode() == LockMode.NONE ? LockMode.READ : getLockMode();

					final EntityEntry entityEntry = persistenceContext.addEntry(
							toInitialize,
							Status.LOADING,
							getResolvedEntityState(),
							rowId,
							getEntityKey().getIdentifier(),
							version,
							lockModeToAcquire,
							true,
							getConcreteDescriptor(),
							false
					);

					updateCaches( toInitialize, rowProcessingState, session, persistenceContext, entityIdentifier, version );
					registerNaturalIdResolution( persistenceContext, entityIdentifier );
					takeSnapshot( rowProcessingState, session, persistenceContext, entityEntry );
					getConcreteDescriptor().afterInitialize( toInitialize, session );
					if ( EntityLoadingLogging.DEBUG_ENABLED ) {
						EntityLoadingLogging.ENTITY_LOADING_LOGGER.debugf(
								"(%s) Done materializing entityInstance : %s",
								getSimpleConcreteImplName(),
								toLoggableString( getNavigablePath(), entityIdentifier )
						);
					}

					final StatisticsImplementor statistics = session.getFactory().getStatistics();
					if ( statistics.isStatisticsEnabled() ) {
						if ( !rowProcessingState.isQueryCacheHit() ) {
							statistics.loadEntity( getConcreteDescriptor().getEntityName() );
						}
					}
				} );
	}
}
