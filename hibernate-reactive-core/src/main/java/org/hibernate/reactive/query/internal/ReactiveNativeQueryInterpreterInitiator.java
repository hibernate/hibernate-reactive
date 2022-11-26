/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.internal;

import org.hibernate.engine.query.spi.NativeQueryInterpreter;
import org.hibernate.reactive.query.sqm.spi.ReactiveNativeQueryInterpreter;
import org.hibernate.service.spi.SessionFactoryServiceInitiator;
import org.hibernate.service.spi.SessionFactoryServiceInitiatorContext;

public class ReactiveNativeQueryInterpreterInitiator implements SessionFactoryServiceInitiator<NativeQueryInterpreter> {

	public static ReactiveNativeQueryInterpreterInitiator INSTANCE = new ReactiveNativeQueryInterpreterInitiator();

	@Override
	public NativeQueryInterpreter initiateService(SessionFactoryServiceInitiatorContext context) {
		return ReactiveNativeQueryInterpreter.INSTANCE;
	}

	@Override
	public Class<NativeQueryInterpreter> getServiceInitiated() {
		return NativeQueryInterpreter.class;
	}
}
