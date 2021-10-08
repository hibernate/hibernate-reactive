/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.common.spi;

import org.hibernate.reactive.stage.Stage;

/**
 * @deprecated It will be removed
 */
@Deprecated
public interface StageImplementor {
	/**
	 * Obtain a new {@link Stage.Session reactive session}, the main
	 * interaction point between the user's program and Hibernate
	 * Reactive.
	 * <p>
	 * The underlying database connection is obtained lazily when
	 * the returned {@link Stage.Session} needs to access the database.
	 * <p>
	 * The client must explicitly close the session by calling
	 * {@link Stage.Session#close()}.
	 *
	 * @see Stage.SessionFactory#withSession(Function)
	 * @see Stage.SessionFactory#openSession()
	 */
	Stage.Session newSession();

	/**
	 * Obtain a new {@link Stage.Session reactive session} for a
	 * specified tenant.
	 * <p>
	 * The underlying database connection is obtained lazily when
	 * the returned {@link Stage.Session} needs to access the database.
	 * <p>
	 * The client must explicitly close the session by calling
	 * {@link Stage.Session#close()}.
	 *
	 * @param tenantId the id of the tenant
	 *
	 * @see Stage.SessionFactory#withSession(Function)
	 * @see Stage.SessionFactory#openSession(String)
	 * @deprecated
	 */
	Stage.Session newSession(String tenantId);

	/**
	 * Obtain a {@link Stage.StatelessSession reactive stateless session}.
	 * <p>
	 * The underlying database connection is obtained lazily when
	 * the returned {@link Stage.StatelessSession} needs to access the
	 * database.
	 * <p>
	 * The client must explicitly close the session by calling
	 * {@link Stage.StatelessSession#close()}.
	 *
	 * @see Stage.SessionFactory#openStatelessSession()
	 * @see Stage.SessionFactory#withStatelessSession(Function)
	 * @deprecated
	 */
	Stage.StatelessSession newStatelessSession();

	/**
	 * Obtain a {@link Stage.StatelessSession reactive stateless session}.
	 * <p>
	 * The underlying database connection is obtained lazily when
	 * the returned {@link Stage.StatelessSession} needs to access the
	 * database.
	 * <p>
	 * The client must explicitly close the session by calling
	 * {@link Stage.StatelessSession#close()}.
	 *
	 * @param tenantId the id of the tenant
	 *
	 * @see Stage.SessionFactory#openStatelessSession(String
	 * @see Stage.SessionFactory#withStatelessSession(String, Function)
	 */
	Stage.StatelessSession newStatelessSession(String tenantId);
}
