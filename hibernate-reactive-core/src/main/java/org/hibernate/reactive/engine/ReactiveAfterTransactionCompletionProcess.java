/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine;

import org.hibernate.engine.spi.SharedSessionContractImplementor;

import java.util.concurrent.CompletionStage;

/**
 * Contract representing some process that needs to occur during after transaction completion.
 *
 * @author Steve Ebersole
 */
public interface ReactiveAfterTransactionCompletionProcess {
	/**
	 * Perform whatever processing is encapsulated here after completion of the transaction.
	 *
	 * @param success Did the transaction complete successfully?  True means it did.
	 * @param session The session on which the transaction is completing.
	 */
	CompletionStage<Void> doAfterTransactionCompletion(boolean success, SharedSessionContractImplementor session);
}
