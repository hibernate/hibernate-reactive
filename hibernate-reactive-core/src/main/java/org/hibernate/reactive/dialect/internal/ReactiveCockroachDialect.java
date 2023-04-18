/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.dialect.internal;

import org.hibernate.dialect.CockroachDialect;

/**
 * The same as {@link CockroachDialect} in Hibernate ORM, but it doesn't require extra
 * queries to parse the version.
 */
public class ReactiveCockroachDialect extends CockroachDialect {

	public ReactiveCockroachDialect(String fullVersion) {
		super( parseVersion( fullVersion ) );
	}
}
