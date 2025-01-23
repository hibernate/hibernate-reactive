/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.spi.AfterLoadAction;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.reactive.loader.ast.spi.ReactiveAfterLoadAction;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.sql.exec.spi.Callback;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;

/**
 * Reactive equivalent of {@link org.hibernate.sql.exec.internal.CallbackImpl}
 */
public class ReactiveCallbackImpl implements Callback {
	private static final Log LOG = make( Log.class, lookup() );

	private final List<ReactiveAfterLoadAction> afterLoadActions;

	public ReactiveCallbackImpl() {
		this.afterLoadActions = new ArrayList<>( 1 );
	}

	@Override
	public void registerAfterLoadAction(AfterLoadAction afterLoadAction) {
		throw LOG.nonReactiveMethodCall( "registerReactiveAfterLoadAction(ReactiveCallbackImpl)" );
	}

	public void registerReactiveAfterLoadAction(ReactiveAfterLoadAction afterLoadAction) {
		afterLoadActions.add( afterLoadAction );
	}

	@Override
	public void invokeAfterLoadActions(
			Object entity,
			EntityMappingType entityMappingType,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "invokeAfterLoadActions(Object, EntityMappingType, SharedSessionContractImplementor)" );
	}

	/**
	 * Reactive version of {@link org.hibernate.sql.exec.internal.CallbackImpl#invokeAfterLoadActions(Object, EntityMappingType, SharedSessionContractImplementor)}
	 */
	public CompletionStage<Void> invokeReactiveLoadActions(
			Object entity,
			EntityMappingType entityMappingType,
			SharedSessionContractImplementor session) {
		return loop(
				afterLoadActions, afterLoadAction ->
						afterLoadAction.reactiveAfterLoad( entity, entityMappingType, session )
		);
	}

	@Override
	public boolean hasAfterLoadActions() {
		return !afterLoadActions.isEmpty();
	}

}
