/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.engine.transaction.jta.platform.internal.NoJtaPlatform;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} for the non-configured form of JTA platform.
 *
 * @see NoJtaPlatform
 */
public class NoJtaPlatformInitiator implements StandardServiceInitiator<JtaPlatform> {
	public static final NoJtaPlatformInitiator INSTANCE = new NoJtaPlatformInitiator();

	@Override
	public JtaPlatform initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return NoJtaPlatform.INSTANCE;
	}

	@Override
	public Class<JtaPlatform> getServiceInitiated() {
		return JtaPlatform.class;
	}
}
