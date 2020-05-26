/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.spi;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.AutoFlushEvent;

public interface ReactiveAutoFlushEventListener extends Serializable {

	CompletionStage<Void> reactiveOnAutoFlush(AutoFlushEvent event) throws HibernateException;

}
