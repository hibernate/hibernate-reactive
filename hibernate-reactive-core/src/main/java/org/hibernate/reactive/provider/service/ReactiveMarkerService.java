/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.service.Service;

/**
 * This service is only used to "mark" the registry as being
 * intended for use by an instance of Hibernate Reactive.
 * @see org.hibernate.reactive.provider.impl.ReactiveModeCheck#isReactiveRegistry(org.hibernate.service.ServiceRegistry)
 */
public interface ReactiveMarkerService extends Service {

}
