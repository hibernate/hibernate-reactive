/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.persister.entity.EntityPersister;

import java.io.Serializable;

public class ReactiveOrphanRemovalAction extends  ReactiveEntityDeleteAction {
    public ReactiveOrphanRemovalAction(Serializable id, Object[] state, Object version, Object instance,
                                       EntityPersister persister, boolean isCascadeDeleteEnabled,
                                       SessionImplementor session) {
        super(id, state, version, instance, persister, isCascadeDeleteEnabled, session);
    }
}
