/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.common.spi;

import java.util.function.Function;

import org.hibernate.reactive.mutiny.Mutiny;

/**
 * @deprecated It will be removed
 */
@Deprecated
public interface MutinyImplementor {

	/**
	 * Obtain a new {@link Mutiny.Session reactive session}, the main
	 * interaction point between the user's program and Hibernate
	 * Reactive.
	 * <p>
	 * The underlying database connection is obtained lazily when
	 * the returned {@link Mutiny.Session} needs to access the database.
	 * <p>
	 * The client must explicitly close the session by calling
	 * {@link Mutiny.Session#close()}.
	 *
	 * @see Mutiny.SessionFactory#withSession(Function)
	 * @see Mutiny.SessionFactory#openSession()
	 */
	Mutiny.Session newSession();

	/**
	 * Obtain a new {@link Mutiny.Session reactive session} for a
	 * specified tenant.
	 * <p>
	 * The underlying database connection is obtained lazily when
	 * the returned {@link Mutiny.Session} needs to access the database.
	 * <p>
	 * The client must explicitly close the session by calling
	 * {@link Mutiny.Session#close()}.
	 *
	 * @param tenantId the id of the tenant
	 *
	 * @see Mutiny.SessionFactory#withSession(String, Function)
	 * @see Mutiny.SessionFactory#openSession(String)
	 */
	Mutiny.Session newSession(String tenantId);

	/**
	 * Obtain a {@link Mutiny.StatelessSession reactive stateless session}.
	 * <p>
	 * The underlying database connection is obtained lazily when
	 * the returned {@link Mutiny.StatelessSession} needs to access the
	 * database.
	 * <p>
	 * The client must explicitly close the session by calling
	 * {@link Mutiny.StatelessSession#close()}.
	 *
	 * @see Mutiny.SessionFactory#openStatelessSession()
	 * @see Mutiny.SessionFactory#withStatelessSession(Function)
	 */
	Mutiny.StatelessSession newStatelessSession();

	/**
	 * Obtain a {@link Mutiny.StatelessSession reactive stateless session}.
	 * <p>
	 * The underlying database connection is obtained lazily when
	 * the returned {@link Mutiny.StatelessSession} needs to access the
	 * database.
	 * <p>
	 * The client must explicitly close the session by calling
	 * {@link Mutiny.StatelessSession#close()}.
	 *
	 * @param tenantId the id of the tenant
	 *
	 * @see Mutiny.SessionFactory#openStatelessSession(String)
	 * @see Mutiny.SessionFactory#withSession(String, Function)
	 */
	Mutiny.StatelessSession newStatelessSession(String tenantId);
}
