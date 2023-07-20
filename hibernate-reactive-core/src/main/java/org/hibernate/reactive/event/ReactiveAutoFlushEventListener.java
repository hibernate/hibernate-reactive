/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event;

import java.io.Serializable;
import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.AutoFlushEvent;

public interface ReactiveAutoFlushEventListener extends Serializable {

	InternalStage<Void> reactiveOnAutoFlush(AutoFlushEvent event) throws HibernateException;

}
