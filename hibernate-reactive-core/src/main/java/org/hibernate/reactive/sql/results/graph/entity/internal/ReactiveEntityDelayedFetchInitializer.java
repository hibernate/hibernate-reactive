/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.bytecode.enhance.spi.LazyPropertyInitializer;
import org.hibernate.engine.spi.EntityHolder;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.EntityUniqueKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.session.impl.ReactiveQueryExecutorLookup;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.results.graph.ReactiveInitializer;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.FetchParentAccess;
import org.hibernate.sql.results.graph.entity.internal.EntityDelayedFetchInitializer;
import org.hibernate.type.Type;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveEntityDelayedFetchInitializer extends EntityDelayedFetchInitializer implements ReactiveInitializer {

	private final ToOneAttributeMapping referencedModelPart;

	public ReactiveEntityDelayedFetchInitializer(
			FetchParentAccess parentAccess,
			NavigablePath fetchedNavigable,
			ToOneAttributeMapping referencedModelPart,
			boolean selectByUniqueKey,
			DomainResultAssembler<?> identifierAssembler) {
		super( parentAccess, fetchedNavigable, referencedModelPart, selectByUniqueKey, identifierAssembler );
		this.referencedModelPart = referencedModelPart;
	}

	@Override
	public CompletionStage<Void> reactiveResolveInstance(ReactiveRowProcessingState rowProcessingState) {
		if ( isProcessed() ) {
			return voidFuture();
		}

		setProcessed( true );

		// We can avoid processing further if the parent is already initialized or missing,
		// as the value produced by this initializer will never be used anyway.
		if ( parentShallowCached || shouldSkipInitializer( rowProcessingState ) ) {
			return voidFuture();
		}

		setIdentifier( getIdentifierAssembler().assemble( rowProcessingState ) );

		CompletionStage<Void> stage = voidFuture();
		if ( getIdentifier() == null ) {
			setEntityInstance( null );
		}
		else {
			final SharedSessionContractImplementor session = rowProcessingState.getSession();
			final EntityPersister concreteDescriptor = referencedModelPart.getEntityMappingType().getEntityPersister();
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			if ( isSelectByUniqueKey() ) {
				final String uniqueKeyPropertyName = referencedModelPart.getReferencedPropertyName();
				final Type uniqueKeyPropertyType = uniqueKeyPropertyName == null
						? concreteDescriptor.getIdentifierType()
						: session.getFactory().getReferencedPropertyType( concreteDescriptor.getEntityName(), uniqueKeyPropertyName );
				final EntityUniqueKey euk = new EntityUniqueKey(
						concreteDescriptor.getEntityName(),
						uniqueKeyPropertyName,
						getIdentifier(),
						uniqueKeyPropertyType,
						session.getFactory()
				);
				setEntityInstance( persistenceContext.getEntity( euk ) );
				if ( getEntityInstance() == null ) {
					// For unique-key mappings, we always use bytecode-laziness if possible,
					// because we can't generate a proxy based on the unique key yet
					if ( referencedModelPart.isLazy() ) {
						setEntityInstance( LazyPropertyInitializer.UNFETCHED_PROPERTY );
					}
					else {
						stage = stage
								.thenCompose( v -> ( (ReactiveEntityPersister) concreteDescriptor )
										.reactiveLoadByUniqueKey( uniqueKeyPropertyName, getIdentifier(), session ) )
								.thenAccept( this::setEntityInstance )
								.thenAccept( v -> {
									// If the entity was not in the Persistence Context, but was found now,
									// add it to the Persistence Context
									if ( getEntityInstance() != null ) {
										persistenceContext.addEntity( euk, getEntityInstance() );
									}
								} );
					}
				}
				stage = stage.thenAccept( v -> {
					if ( getEntityInstance() != null ) {
						setEntityInstance( persistenceContext.proxyFor( getEntityInstance() ) );
					}
				} );
			}
			else {
				final EntityKey entityKey = new EntityKey( getIdentifier(), concreteDescriptor );
				final EntityHolder holder = persistenceContext.getEntityHolder( entityKey );
				if ( holder != null && holder.getEntity() != null ) {
					setEntityInstance( persistenceContext.proxyFor( holder, concreteDescriptor ) );
				}
				// For primary key based mappings we only use bytecode-laziness if the attribute is optional,
				// because the non-optionality implies that it is safe to have a proxy
				else if ( referencedModelPart.isOptional() && referencedModelPart.isLazy() ) {
					setEntityInstance( LazyPropertyInitializer.UNFETCHED_PROPERTY );
				}
				else {
					stage = stage.thenCompose( v -> ReactiveQueryExecutorLookup
							.extract( session )
							.reactiveInternalLoad( concreteDescriptor.getEntityName(), getIdentifier(), false, false )
							.thenAccept( this::setEntityInstance )
					);
				}
				stage = stage
						.thenAccept( v -> {
							final LazyInitializer lazyInitializer = HibernateProxy.extractLazyInitializer( getEntityInstance() );
							if ( lazyInitializer != null ) {
								lazyInitializer.setUnwrap( referencedModelPart.isUnwrapProxy() && concreteDescriptor.isInstrumented() );
							}
						} );
			}
		}
		return stage;
	}

	@Override
	public CompletionStage<Void> reactiveInitializeInstance(ReactiveRowProcessingState rowProcessingState) {
		return voidFuture();
	}
}
