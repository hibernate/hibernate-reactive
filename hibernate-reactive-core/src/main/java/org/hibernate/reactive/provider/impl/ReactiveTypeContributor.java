/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.impl;

import io.vertx.core.json.JsonObject;
import org.hibernate.boot.model.TypeContributions;
import org.hibernate.boot.model.TypeContributor;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.PostgreSQL10Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.AbstractSingleColumnStandardBasicType;
import org.hibernate.type.BasicTypeRegistry;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.ValueExtractor;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaTypeDescriptor;
import org.hibernate.type.descriptor.java.PrimitiveByteArrayTypeDescriptor;
import org.hibernate.type.descriptor.java.StringTypeDescriptor;
import org.hibernate.type.descriptor.sql.BasicBinder;
import org.hibernate.type.descriptor.sql.BasicExtractor;
import org.hibernate.type.descriptor.sql.SqlTypeDescriptor;
import org.hibernate.type.descriptor.sql.VarbinaryTypeDescriptor;
import org.hibernate.type.descriptor.sql.VarcharTypeDescriptor;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

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
		Dialect dialect = serviceRegistry.getService(JdbcEnvironment.class).getDialect();
		basicTypeRegistry.register( new BlobType(dialect) );
		basicTypeRegistry.register( new ClobType(dialect) );
		basicTypeRegistry.register( new JsonType(dialect) );
	}

	private static class JsonType extends AbstractSingleColumnStandardBasicType<JsonObject> {
		public JsonType(Dialect dialect) {
			super(new SqlTypeDescriptor() {
				@Override
				public int getSqlType() {
					// use Types.JAVA_OBJECT instead of Types.OTHER because
					// the Dialects have the type 'json' registered under
					// that JDBC type code
					return Types.JAVA_OBJECT;
				}

				@Override
				public boolean canBeRemapped() {
					return true;
				}

				@Override
				public <X> ValueBinder<X> getBinder(JavaTypeDescriptor<X> javaTypeDescriptor) {
					return new BasicBinder<X>(javaTypeDescriptor, this) {
						@Override
						protected void doBind(PreparedStatement st, X value, int index, WrapperOptions options) throws SQLException {
							st.setObject( index, javaTypeDescriptor.unwrap(value, JsonObject.class, options) );
						}
						@Override
						protected void doBind(CallableStatement st, X value, String name, WrapperOptions options) throws SQLException {
							throw new UnsupportedOperationException();
						}
					};
				}

				@Override
				public <X> ValueExtractor<X> getExtractor(JavaTypeDescriptor<X> javaTypeDescriptor) {
					return new BasicExtractor<X>(javaTypeDescriptor, this) {
						@Override
						protected X doExtract(ResultSet rs, String name, WrapperOptions options) throws SQLException {
							Object result = rs.getObject(name);
							// Currently Vertx does not return JsonObject type from MariaDb so adding type check to convert the String value
							if ( result instanceof String ) {
								return javaTypeDescriptor.wrap( new JsonObject( result.toString() ), options );
							}
							return javaTypeDescriptor.wrap( result, options );
						}

						@Override
						protected X doExtract(CallableStatement statement, int index, WrapperOptions options) throws SQLException {
							throw new UnsupportedOperationException();
						}

						@Override
						protected X doExtract(CallableStatement statement, String name, WrapperOptions options) throws SQLException {
							throw new UnsupportedOperationException();
						}
					};
				}
			},
			new JavaTypeDescriptor<JsonObject>() {
				@Override
				public Class<JsonObject> getJavaTypeClass() {
					return JsonObject.class;
				}

				@Override
				public JsonObject fromString(String string) {
					throw new UnsupportedOperationException();
				}

				@Override
				public <X> X unwrap(JsonObject value, Class<X> type, WrapperOptions options) {
					return (X) value;
				}

				@Override
				public <X> JsonObject wrap(X value, WrapperOptions options) {
					return (JsonObject) value;
				}
			});
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
			super(new VarcharTypeDescriptor() {
				@Override
				public int getSqlType() {
					//force the use of byte instead of oid on Postgres
					return dialect instanceof PostgreSQL10Dialect
							? Types.LONGVARCHAR
							: Types.CLOB;
				}

				@Override
				public boolean canBeRemapped() {
					return false;
				}
			}, StringTypeDescriptor.INSTANCE);
		}

		@Override
		public String getName() {
			return "materialized_clob";
		}
	}

	private static class BlobType extends AbstractSingleColumnStandardBasicType<byte[]> {
		public BlobType(Dialect dialect) {
			super(new VarbinaryTypeDescriptor() {
				@Override
				public int getSqlType() {
					//force the use of text instead of oid on Postgres
					return dialect instanceof PostgreSQL10Dialect
							? Types.LONGVARBINARY
							: Types.BLOB;
				}

				@Override
				public boolean canBeRemapped() {
					return false;
				}
			}, PrimitiveByteArrayTypeDescriptor.INSTANCE);
		}

		@Override
		public String getName() {
			return "materialized_blob";
		}
	}
}
