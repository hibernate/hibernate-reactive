/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import org.hibernate.action.internal.EntityAction;
import org.hibernate.action.internal.EntityActionVetoException;
import org.hibernate.reactive.engine.ReactiveExecutable;

public class ReactiveEntityActionVetoException extends EntityActionVetoException {

		private final ReactiveExecutable reactiveAction;

		/**
		 * Constructs a ReactiveEntityActionVetoException
		 *
		 * @param message Message explaining the exception condition
		 * @param reactiveAction The {@link ReactiveExecutable} was vetoed that was vetoed.
		 */
		public ReactiveEntityActionVetoException(String message, ReactiveExecutable reactiveAction) {
			super( message, entityAction( reactiveAction ) );
			this.reactiveAction = reactiveAction;
		}

		private static EntityAction entityAction(ReactiveExecutable reactiveAction) {
			// I think this is always true
			return reactiveAction instanceof EntityAction
					? (EntityAction) reactiveAction
					: null;
		}

		public ReactiveExecutable getReactiveAction() {
			return reactiveAction;
		}
	}
