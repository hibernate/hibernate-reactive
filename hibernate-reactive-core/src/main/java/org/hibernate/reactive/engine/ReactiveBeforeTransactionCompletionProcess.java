/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine;

import org.hibernate.reactive.session.ReactiveSession;

import java.util.concurrent.CompletionStage;

/**
 * Contract representing some process that needs to occur during before transaction completion.
 *
 * @author Gavin King
 * @author Steve Ebersole
 */
public interface ReactiveBeforeTransactionCompletionProcess {
	/**
	 * Perform whatever processing is encapsulated here before completion of the transaction.
	 *
	 * @param session The session on which the transaction is preparing to complete.
	 */
	CompletionStage<Void> doBeforeTransactionCompletion(ReactiveSession session);
}
