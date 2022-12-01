/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.iternal;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.QueryParameterImplementor;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SimpleDeleteQueryPlan;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.expression.SqmParameter;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.exec.spi.JdbcOperationQueryDelete;

;

/**
 * @see
 */
public class ReactiveSimpleDeleteQueryPlan extends SimpleDeleteQueryPlan implements ReactiveNonSelectQueryPlan {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private JdbcOperationQueryDelete jdbcDelete;
	private EntityMappingType entityDescriptor;
	private SqmDeleteStatement<?> sqmDelete;
	private DomainParameterXref domainParameterXref;

	private Map<QueryParameterImplementor<?>, Map<SqmParameter<?>, List<List<JdbcParameter>>>> jdbcParamsXref;

	public ReactiveSimpleDeleteQueryPlan(
			EntityMappingType entityDescriptor,
			SqmDeleteStatement<?> sqmDelete,
			DomainParameterXref domainParameterXref) {
		super( entityDescriptor, sqmDelete, domainParameterXref );
		this.entityDescriptor = entityDescriptor;
		this.sqmDelete = sqmDelete;
		this.domainParameterXref = domainParameterXref;
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate(DomainQueryExecutionContext executionContext) {
		return null;
	}
}
