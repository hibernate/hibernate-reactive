/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.impl;

import java.lang.reflect.Type;
import java.sql.Types;

import org.hibernate.boot.model.TypeContributions;
import org.hibernate.boot.model.TypeContributor;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.AbstractSingleColumnStandardBasicType;
import org.hibernate.type.BasicTypeRegistry;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.ValueExtractor;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.BasicJavaType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.java.PrimitiveByteArrayJavaType;
import org.hibernate.type.descriptor.java.StringJavaType;
import org.hibernate.type.descriptor.java.spi.JavaTypeRegistry;
import org.hibernate.type.descriptor.jdbc.BlobJdbcType;
import org.hibernate.type.descriptor.jdbc.ClobJdbcType;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.JdbcTypeIndicators;
import org.hibernate.type.descriptor.jdbc.ObjectJdbcType;
import org.hibernate.type.descriptor.sql.DdlType;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;

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
		DdlTypeRegistry ddlTypeRegistry = typeContributions.getTypeConfiguration().getDdlTypeRegistry();
		ddlTypeRegistry.addDescriptor( Types.JAVA_OBJECT, new JavaObjectDdlType() );

		JavaTypeRegistry javaTypeRegistry = typeContributions.getTypeConfiguration().getJavaTypeRegistry();
		javaTypeRegistry.addDescriptor( JsonObjectJavaType.INSTANCE );

		Dialect dialect = serviceRegistry.getService( JdbcEnvironment.class ).getDialect();
		basicTypeRegistry.register( new JsonType( dialect ) );
		// FIXME: I think only Postgres, needs this special type because of the way the driver returns the SQL type
		// 		  We could only add them for Postgres
		basicTypeRegistry.register( new BlobType( dialect ) );
		basicTypeRegistry.register( new ClobType( dialect ) );
	}

	private static class JavaObjectDdlType implements DdlType {

		@Override
		public int getSqlTypeCode() {
			return Types.JAVA_OBJECT;
		}

		@Override
		public String getRawTypeName() {
			return "json";
		}

		@Override
		public String getTypeNamePattern() {
			return "";
		}

		@Override
		public String getTypeName(Long size, Integer precision, Integer scale) {
			return "json";
		}

		@Override
		public String getCastTypeName(JdbcType jdbcType, JavaType<?> javaType) {
			return "json";
		}

		@Override
		public String getCastTypeName(JdbcType jdbcType, JavaType<?> javaType, Long length, Integer precision, Integer scale) {
			return "json";
		}
	}

	private static class JsonObjectJavaType implements BasicJavaType<JsonObject> {

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
		public JavaType<?> getJdbcJavaType() {
			return super.getJdbcJavaType();
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

	private static class ClobType extends AbstractSingleColumnStandardBasicType<String> {
		public ClobType(Dialect dialect) {
			super( new ReactiveClobJdbcType( dialect ), StringJavaType.INSTANCE );
		}

		@Override
		public String getName() {
			return "materialized_clob";
		}
	}

	private static class ReactiveClobJdbcType implements JdbcType {

		private JdbcType delegate = ClobJdbcType.DEFAULT;

		private final int defaultSqlTypeCode;

		public ReactiveClobJdbcType(Dialect dialect) {
			defaultSqlTypeCode = dialect instanceof PostgreSQLDialect
					? Types.LONGVARCHAR
					: Types.CLOB;
		}

		@Override
		public Class<?> getPreferredJavaTypeClass(WrapperOptions options) {
			return delegate.getPreferredJavaTypeClass( options );
		}

		@Override
		public int getJdbcTypeCode() {
			return delegate.getJdbcTypeCode();
		}

		@Override
		public int getDefaultSqlTypeCode() {
			return defaultSqlTypeCode;
		}

		@Override
		public <X> ValueBinder<X> getBinder(JavaType<X> javaType) {
			return delegate.getBinder( javaType );
		}

		@Override
		public <X> ValueExtractor<X> getExtractor(JavaType<X> javaType) {
			return delegate.getExtractor( javaType );
		}
	}

	private static class BlobType extends AbstractSingleColumnStandardBasicType<byte[]> {
		public BlobType(Dialect dialect) {
			super( new ReactiveBlobJdbcType( dialect ), PrimitiveByteArrayJavaType.INSTANCE );
		}

		@Override
		public String getName() {
			return "materialized_blob";
		}
	}

	private static class ReactiveBlobJdbcType implements JdbcType {

		private JdbcType delegate = BlobJdbcType.DEFAULT;

		private final int defaultSqlTypeCode;

		public ReactiveBlobJdbcType(Dialect dialect) {
			defaultSqlTypeCode = dialect instanceof PostgreSQLDialect
					? Types.LONGVARCHAR
					: Types.BLOB;
		}

		@Override
		public Class<?> getPreferredJavaTypeClass(WrapperOptions options) {
			return delegate.getPreferredJavaTypeClass( options );
		}

		@Override
		public int getJdbcTypeCode() {
			return delegate.getJdbcTypeCode();
		}

		@Override
		public int getDefaultSqlTypeCode() {
			return defaultSqlTypeCode;
		}

		@Override
		public <X> ValueBinder<X> getBinder(JavaType<X> javaType) {
			return delegate.getBinder( javaType );
		}

		@Override
		public <X> ValueExtractor<X> getExtractor(JavaType<X> javaType) {
			return delegate.getExtractor( javaType );
		}
	}
}
