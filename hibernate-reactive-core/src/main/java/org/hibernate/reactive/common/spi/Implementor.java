/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.common.spi;

import org.hibernate.reactive.context.Context;
import org.hibernate.service.ServiceRegistry;

/**
 * Allows access to object that can be useful for integrators
 */
public interface Implementor {

	ServiceRegistry getServiceRegistry();

	Context getContext();
}
