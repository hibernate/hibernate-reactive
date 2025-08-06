/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.cte;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.util.MutableObject;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.internal.cte.CteMutationStrategy;
import org.hibernate.query.sqm.mutation.internal.cte.CteSoftDeleteHandler;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.sql.ast.tree.cte.CteTable;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

public class ReactiveCteSoftDeleteHandler extends CteSoftDeleteHandler implements ReactiveAbstractCteMutationHandler {
	protected ReactiveCteSoftDeleteHandler(
			CteTable cteTable,
			SqmDeleteStatement<?> sqmDeleteStatement,
			DomainParameterXref domainParameterXref,
			CteMutationStrategy strategy,
			SessionFactoryImplementor sessionFactory,
			DomainQueryExecutionContext context,
			MutableObject<JdbcParameterBindings> firstJdbcParameterBindingsConsumer) {
		super(
				cteTable,
				sqmDeleteStatement,
				domainParameterXref,
				strategy,
				sessionFactory,
				context,
				firstJdbcParameterBindingsConsumer
		);
	}

	@Override
	public JdbcOperationQuerySelect getSelect() {
		return super.getSelect();
	}
}
