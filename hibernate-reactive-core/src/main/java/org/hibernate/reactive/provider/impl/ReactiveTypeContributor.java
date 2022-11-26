/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.impl;

import java.lang.reflect.Type;

import org.hibernate.boot.model.TypeContributions;
import org.hibernate.boot.model.TypeContributor;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.AbstractSingleColumnStandardBasicType;
import org.hibernate.type.BasicTypeRegistry;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.java.spi.JavaTypeRegistry;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.JdbcTypeIndicators;
import org.hibernate.type.descriptor.jdbc.ObjectJdbcType;

import io.vertx.core.json.JsonObject;

/**
 * Overrides Hibernate's built-in `materialized_blob` and `materialized_clob`
 * type mappings and replaces them with the same handling as regular byte
 * arrays and strings, since the {@link io.vertx.sqlclient.SqlClient} doesn't
 * support any special handling for LOBs.
 * This is only applied if the Hibernate ORM instance we're registering with
 * is marked as being reactive.
 */
public class ReactiveTypeContributor implements TypeContributor {

	@Override
	public void contribute(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
		if ( ReactiveModeCheck.isReactiveRegistry( serviceRegistry ) ) {
			registerReactiveChanges( typeContributions, serviceRegistry );
		}
	}

	private void registerReactiveChanges(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
		BasicTypeRegistry basicTypeRegistry = typeContributions.getTypeConfiguration().getBasicTypeRegistry();

		JavaTypeRegistry javaTypeRegistry = typeContributions.getTypeConfiguration().getJavaTypeRegistry();
		javaTypeRegistry.addDescriptor( JsonObjectJavaType.INSTANCE );

		Dialect dialect = serviceRegistry.getService( JdbcEnvironment.class ).getDialect();
		// FIXME: [ORM6] I don't think we need these two anymore
//		basicTypeRegistry.register( new BlobType( dialect ) );
//		basicTypeRegistry.register( new ClobType( dialect ) );
		basicTypeRegistry.register( new JsonType( dialect ) );
	}

	private static class JsonObjectJavaType implements JavaType<JsonObject> {

		public static final JsonObjectJavaType INSTANCE = new JsonObjectJavaType();

		@Override
		public JdbcType getRecommendedJdbcType(JdbcTypeIndicators context) {
			// FIXME: Check this
			// use Types.JAVA_OBJECT instead of Types.JSON because
			// the Dialects have the type 'json' registered under
			// that JDBC type code
			return ObjectJdbcType.INSTANCE;
		}

		@Override
		public Type getJavaType() {
			return JsonObject.class;
		}

		@Override
		public JsonObject fromString(CharSequence string) {
			return string == null ? null : new JsonObject( String.valueOf( string ) );
		}

		@Override
		public <X> X unwrap(JsonObject value, Class<X> type, WrapperOptions options) {
			return (X) value;
		}

		@Override
		public <X> JsonObject wrap(X value, WrapperOptions options) {
			return (JsonObject) value;
		}
	}

	private static class JsonType extends AbstractSingleColumnStandardBasicType<JsonObject> {
		public JsonType(Dialect dialect) {
			super( ObjectJdbcType.INSTANCE, JsonObjectJavaType.INSTANCE );
		}

		@Override
		public String getName() {
			return "json";
		}


		@Override
		public String[] getRegistrationKeys() {
			return new String[] { "json", JsonObject.class.getName() };
		}
	}
}
