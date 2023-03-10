/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.DialectDelegateWrapper;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.sql.ast.spi.JdbcParameterRenderer;
import org.hibernate.type.descriptor.jdbc.JdbcType;

/**
 * Replaces the JdbcParameterRendererInitiator so to not require
 * users to set AvailableSettings.DIALECT_NATIVE_PARAM_MARKERS : this
 * gets enforces as the Vert.x SQL clients require it.
 */
public class NativeParametersRendering implements StandardServiceInitiator<JdbcParameterRenderer> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	/**
	 * Singleton access
	 */
	public static final NativeParametersRendering INSTANCE = new NativeParametersRendering();

	@Override
	public JdbcParameterRenderer initiateService(Map<String, Object> configurationValues, ServiceRegistryImplementor registry) {
		final Dialect dialect = registry.getService( JdbcServices.class ).getDialect();
		final Dialect realDialect = DialectDelegateWrapper.extractRealDialect( dialect );
		final JdbcParameterRenderer renderer = recommendRendered( realDialect );
		LOG.debug( "Initializing service JdbcParameterRenderer with implementation: " + renderer.getClass() );
		return renderer;
	}

	private JdbcParameterRenderer recommendRendered(Dialect realDialect) {
		if ( realDialect instanceof PostgreSQLDialect ) {
			return new PostgreSQLNativeParameterMarkers();
		}
		//TBD : Implementations for other DBs
		else {
			return realDialect.getNativeParameterRenderer();
		}
	}

	@Override
	public Class<JdbcParameterRenderer> getServiceInitiated() {
		return JdbcParameterRenderer.class;
	}

	private static class PostgreSQLNativeParameterMarkers implements JdbcParameterRenderer {
		@Override
		public String renderJdbcParameter(int position, JdbcType jdbcType) {
			return "$" + position;
		}
	}

}
