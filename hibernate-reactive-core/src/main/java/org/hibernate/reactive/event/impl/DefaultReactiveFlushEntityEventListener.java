/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.internal.DefaultFlushEntityEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.FlushEntityEvent;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.impl.ReactiveEntityUpdateAction;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveSession;

import java.lang.invoke.MethodHandles;

/**
 * A reactific {@link DefaultFlushEntityEventListener}.
 * The only difference is that it creates
 * {@link ReactiveEntityUpdateAction}s. Unlike other event listeners in this package, this
 * listener's {@link #onFlushEntity(FlushEntityEvent)} method does not need to by
 * called in a non-blocking manner, and so therefore there is no
 * {@code ReactiveFlushEntityEventListener} interface.
 */
public class DefaultReactiveFlushEntityEventListener extends DefaultFlushEntityEventListener {

	protected static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	protected void addEntityUpdateActionToActionQueue(FlushEntityEvent event, EventSource session, EntityEntry entry, Object[] values, int[] dirtyProperties, Status status, EntityPersister persister, Object entity, Object nextVersion) {
		// schedule the update
		// note that we intentionally do _not_ pass in currentPersistentState!
		session.unwrap(ReactiveSession.class).getReactiveActionQueue().addAction(
				new ReactiveEntityUpdateAction(
						entry.getId(),
						values,
						dirtyProperties,
						event.hasDirtyCollection(),
						status == Status.DELETED && !entry.isModifiableEntity()
								? persister.getValues( entity )
								: entry.getLoadedState(),
						entry.getVersion(),
						nextVersion,
						entity,
						entry.getRowId(),
						persister,
						session
				)
		);
	}

	@Override
	protected void throwIdentifiedAlteredException(EntityPersister persister, Object entryId, Object currentId) {
		throw LOG.identifierAltered( persister.getEntityName(), currentId, entryId );
	}
}
