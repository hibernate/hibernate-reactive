/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.enhanced;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.enhanced.DatabaseStructure;

public interface ReactiveDatabaseStructure extends DatabaseStructure {
	ReactiveAccessCallback buildReactiveCallback(SharedSessionContractImplementor session);
}
