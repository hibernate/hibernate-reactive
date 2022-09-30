/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import org.hibernate.event.spi.PersistContext;
import org.hibernate.reactive.engine.impl.CascadingAction;
import org.hibernate.reactive.engine.impl.CascadingActions;

/**
 * A reactific {@link org.hibernate.event.internal.DefaultPersistOnFlushEventListener}.
 */
public class DefaultReactivePersistOnFlushEventListener extends DefaultReactivePersistEventListener {

	@Override
	protected CascadingAction<PersistContext> getCascadeReactiveAction() {
		return CascadingActions.PERSIST_ON_FLUSH;
	}
}
