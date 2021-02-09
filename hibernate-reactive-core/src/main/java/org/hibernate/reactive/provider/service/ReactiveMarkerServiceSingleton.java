/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;


/**
 * A singleton {@link ReactiveMarkerService} that marks the registry as running in "Reactive mode" allowing
 * the registration of reactive components.
 */
public final class ReactiveMarkerServiceSingleton implements ReactiveMarkerService {

	public static final ReactiveMarkerServiceSingleton INSTANCE = new ReactiveMarkerServiceSingleton();

	private ReactiveMarkerServiceSingleton(){}

}
