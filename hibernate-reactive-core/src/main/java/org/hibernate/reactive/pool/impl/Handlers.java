/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * Utility operations for working with Vert.x handlers
 */
public class Handlers {

	protected static <T> CompletionStage<T> toCompletionStage(
			Consumer<Handler<AsyncResult<T>>> completionConsumer) {
		CompletableFuture<T> cs = new CompletableFuture<>();
//		try {
			completionConsumer.accept( ar -> {
				if ( ar.succeeded() ) {
					cs.complete( ar.result() );
				}
				else {
					cs.completeExceptionally( ar.cause() );
				}
			} );
//		}
//		catch (Exception e) {
//			cs.completeExceptionally( e );
//		}
		return cs;
	}
}
