/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.reactive.engine.impl.CascadingAction;
import org.hibernate.reactive.engine.impl.CascadingActions;

/**
 * A reactific {@link org.hibernate.event.internal.DefaultPersistOnFlushEventListener}.
 */
public class DefaultReactivePersistOnFlushEventListener extends DefaultReactivePersistEventListener {
	@Override
	protected CascadingAction<IdentitySet> getCascadeReactiveAction() {
		return CascadingActions.PERSIST_ON_FLUSH;
	}
}
