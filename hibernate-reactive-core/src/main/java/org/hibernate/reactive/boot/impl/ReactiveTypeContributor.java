/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.boot.impl;

import org.hibernate.boot.model.TypeContributions;
import org.hibernate.boot.model.TypeContributor;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.PostgreSQL10Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.AbstractSingleColumnStandardBasicType;
import org.hibernate.type.BasicTypeRegistry;
import org.hibernate.type.descriptor.java.PrimitiveByteArrayTypeDescriptor;
import org.hibernate.type.descriptor.java.StringTypeDescriptor;
import org.hibernate.type.descriptor.sql.VarbinaryTypeDescriptor;
import org.hibernate.type.descriptor.sql.VarcharTypeDescriptor;

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
