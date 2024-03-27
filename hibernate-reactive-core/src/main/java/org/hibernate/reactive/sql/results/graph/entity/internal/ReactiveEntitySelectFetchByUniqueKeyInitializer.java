/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.graph.entity.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.EntityUniqueKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.ForeignKeyDescriptor;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.spi.EntityIdentifierNavigablePath;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.FetchParentAccess;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.whileLoop;

/**
 * @see org.hibernate.sql.results.graph.entity.internal.EntitySelectFetchByUniqueKeyInitializer
 */
public class ReactiveEntitySelectFetchByUniqueKeyInitializer extends ReactiveEntitySelectFetchInitializer {

	private final ToOneAttributeMapping fetchedAttribute;

	public ReactiveEntitySelectFetchByUniqueKeyInitializer(
			FetchParentAccess parentAccess,
			ToOneAttributeMapping fetchedAttribute,
			NavigablePath fetchedNavigable,
			EntityPersister concreteDescriptor,
			DomainResultAssembler<?> keyAssembler) {
		super( parentAccess, fetchedAttribute, fetchedNavigable, concreteDescriptor, keyAssembler );
		this.fetchedAttribute = fetchedAttribute;
	}

	@Override
	public CompletionStage<Void> reactiveResolveInstance(ReactiveRowProcessingState rowProcessingState) {
		if ( state != State.UNINITIALIZED ) {
			return voidFuture();
		}
		state = State.RESOLVED;

		// We can avoid processing further if the parent is already initialized or missing,
		// as the value produced by this initializer will never be used anyway.
		if ( parentShallowCached || shouldSkipInitializer( rowProcessingState ) ) {
			state = State.INITIALIZED;
			return voidFuture();
		}

		entityIdentifier = keyAssembler.assemble( rowProcessingState );
		if ( entityIdentifier == null ) {
			state = State.INITIALIZED;
			return voidFuture();
		}

		NavigablePath[] np = { getNavigablePath().getParent() };
		if ( np[0] == null ) {
			return voidFuture();
		}
		return whileLoop( () -> {
			CompletionStage<Void> loop = voidFuture();
			// Defer the select by default to the initialize phase
			// We only need to select in this phase if this is part of an identifier or foreign key
			if ( np[0] instanceof EntityIdentifierNavigablePath
					|| ForeignKeyDescriptor.PART_NAME.equals( np[0].getLocalName() )
					|| ForeignKeyDescriptor.TARGET_PART_NAME.equals( np[0].getLocalName() ) ) {
				loop = reactiveInitializeInstance( rowProcessingState );
			}
			return loop.thenApply( v -> {
				np[0] = np[0].getParent();
				return np[0] != null;
			} );
		} );
	}

	@Override
	public CompletionStage<Void> reactiveInitializeInstance(ReactiveRowProcessingState rowProcessingState) {
		if ( state != State.RESOLVED ) {
			return voidFuture();
		}
		state = State.INITIALIZED;

		final String entityName = concreteDescriptor.getEntityName();
		final String uniqueKeyPropertyName = fetchedAttribute.getReferencedPropertyName();
		final SharedSessionContractImplementor session = rowProcessingState.getSession();
		final EntityUniqueKey euk = new EntityUniqueKey(
				entityName,
				uniqueKeyPropertyName,
				entityIdentifier,
				concreteDescriptor.getPropertyType( uniqueKeyPropertyName ),
				session.getFactory()
		);
		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		setEntityInstance( persistenceContext.getEntity( euk ) );
		if ( entityInstance == null ) {
			return ( (ReactiveEntityPersister) concreteDescriptor )
					.reactiveLoadByUniqueKey( uniqueKeyPropertyName, entityIdentifier, session )
					.thenAccept( this::setEntityInstance )
					.thenAccept( v -> {
						// If the entity was not in the Persistence Context, but was found now,
						// add it to the Persistence Context
						if ( entityInstance != null ) {
							persistenceContext.addEntity( euk, entityInstance );
						}
					} );
		}
		if ( entityInstance != null ) {
			setEntityInstance( persistenceContext.proxyFor( entityInstance ) );
		}
		return voidFuture();
	}

	private void setEntityInstance(Object instance) {
		entityInstance = instance;
	}
}
