/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.boot.impl.marker;

import org.hibernate.reactive.boot.impl.ReactiveModeCheck;
import org.hibernate.service.Service;

/**
 * This service is only used to "mark" the registry as being
 * intended for use by an instance of Hibernate Reactive.
 * @see ReactiveModeCheck#isReactiveRegistry(org.hibernate.service.ServiceRegistry)
 */
public interface ReactiveMarkerService extends Service {

}
