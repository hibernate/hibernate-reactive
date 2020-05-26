/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.boot.impl;

import org.hibernate.boot.model.TypeContributions;
import org.hibernate.boot.model.TypeContributor;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.AbstractSingleColumnStandardBasicType;
import org.hibernate.type.BasicTypeRegistry;
import org.hibernate.type.descriptor.java.PrimitiveByteArrayTypeDescriptor;
import org.hibernate.type.descriptor.java.StringTypeDescriptor;
import org.hibernate.type.descriptor.sql.VarbinaryTypeDescriptor;
import org.hibernate.type.descriptor.sql.VarcharTypeDescriptor;

/**
 * Overrides Hibernate's built-in `materialized_blob` and `materialized_clob`
 * type mappings and replaces them with the same handling as regular byte
 * arrays and strings, since the {@link io.vertx.sqlclient.SqlClient} doesn't
 * support anything special handling for LOBs.
 */
public class ReactiveTypeContributor implements TypeContributor {
	@Override
	public void contribute(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
		BasicTypeRegistry basicTypeRegistry =
				typeContributions.getTypeConfiguration().getBasicTypeRegistry();
		basicTypeRegistry.register(new AbstractSingleColumnStandardBasicType<byte[]>(
				VarbinaryTypeDescriptor.INSTANCE,
				PrimitiveByteArrayTypeDescriptor.INSTANCE
		) {
			@Override
			public String getName() {
				return "materialized_blob";
			}
			@Override
			protected boolean registerUnderJavaType() {
				return true;
			}
		});
		basicTypeRegistry.register(new AbstractSingleColumnStandardBasicType<String>(
				VarcharTypeDescriptor.INSTANCE,
				StringTypeDescriptor.INSTANCE
		) {
			@Override
			public String getName() {
				return "materialized_clob";
			}
			@Override
			protected boolean registerUnderJavaType() {
				return true;
			}
		});
	}
}
