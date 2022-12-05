/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import org.hibernate.event.spi.EventSource;
import org.hibernate.persister.entity.EntityPersister;

public class ReactiveOrphanRemovalAction extends ReactiveEntityDeleteAction {
    public ReactiveOrphanRemovalAction(
            Object id,
            Object[] state,
            Object version,
            Object instance,
            EntityPersister persister,
            boolean isCascadeDeleteEnabled,
            EventSource session) {
        super( id, state, version, instance, persister, isCascadeDeleteEnabled, session );
    }
}
