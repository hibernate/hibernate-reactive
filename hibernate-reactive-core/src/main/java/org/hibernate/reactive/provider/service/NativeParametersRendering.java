/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.sql.ast.spi.JdbcParameterRenderer;

/**
 * Replaces the JdbcParameterRendererInitiator so to not require
 * users to set AvailableSettings.DIALECT_NATIVE_PARAM_MARKERS : this
 * gets enforces as the Vert.x SQL clients require it.
 */
public class NativeParametersRendering implements StandardServiceInitiator<JdbcParameterRenderer> {
	/**
	 * Singleton access
	 */
	public static final NativeParametersRendering INSTANCE = new NativeParametersRendering();

	@Override
	public JdbcParameterRenderer initiateService(Map<String, Object> configurationValues, ServiceRegistryImplementor registry) {
		final Dialect dialect = registry.getService( JdbcServices.class ).getDialect();
		return dialect.getNativeParameterRenderer();
	}

	@Override
	public Class<JdbcParameterRenderer> getServiceInitiated() {
		return JdbcParameterRenderer.class;
	}

}
