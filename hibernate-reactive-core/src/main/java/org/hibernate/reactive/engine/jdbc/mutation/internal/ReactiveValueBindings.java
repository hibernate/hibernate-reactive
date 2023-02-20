/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import java.lang.invoke.MethodHandles;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.hibernate.engine.jdbc.mutation.ParameterUsage;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.group.UnknownParameterException;
import org.hibernate.engine.jdbc.mutation.internal.JdbcValueBindingsImpl;
import org.hibernate.engine.jdbc.mutation.spi.BindingGroup;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.sql.model.MutationTarget;
import org.hibernate.sql.model.MutationType;
import org.hibernate.sql.model.TableMapping;
import org.hibernate.sql.model.jdbc.JdbcValueDescriptor;

/**
 * @see org.hibernate.engine.jdbc.mutation.JdbcValueBindings
 */
public class ReactiveValueBindings extends JdbcValueBindingsImpl {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final MutationType mutationType;
	private final MutationTarget<?> mutationTarget;
	private final JdbcValueBindingsImpl.JdbcValueDescriptorAccess jdbcValueDescriptorAccess;

	private final SharedSessionContractImplementor session;

	private final Map<String, BindingGroup> bindingGroupMap = new HashMap<>();

	public ReactiveValueBindings(MutationType mutationType, MutationTarget<?> mutationTarget, JdbcValueBindingsImpl.JdbcValueDescriptorAccess jdbcValueDescriptorAccess, SharedSessionContractImplementor session) {
		super( mutationType, mutationTarget, jdbcValueDescriptorAccess, session );
		this.mutationType = mutationType;
		this.mutationTarget = mutationTarget;
		this.jdbcValueDescriptorAccess = jdbcValueDescriptorAccess;
		this.session = session;
	}

	@Override
	public void bindValue(Object value, String tableName, String columnName, ParameterUsage usage) {
		final JdbcValueDescriptor jdbcValueDescriptor = jdbcValueDescriptorAccess.resolveValueDescriptor( tableName, columnName, usage );
		if ( jdbcValueDescriptor == null ) {
			throw new UnknownParameterException( mutationType, mutationTarget, tableName, columnName, usage );
		}

		resolveBindingGroup( tableName ).bindValue( columnName, value, jdbcValueDescriptor );
	}

	private BindingGroup resolveBindingGroup(String tableName) {
		final BindingGroup existing = bindingGroupMap.get( tableName );
		if ( existing != null ) {
			assert tableName.equals( existing.getTableName() );
			return existing;
		}

		final BindingGroup created = new BindingGroup( tableName );
		bindingGroupMap.put( tableName, created );
		return created;
	}

	@Override
	public void beforeStatement(PreparedStatementDetails statementDetails) {
		final BindingGroup bindingGroup = bindingGroupMap.get( statementDetails.getMutatingTableDetails().getTableName() );
		if ( bindingGroup == null ) {
			statementDetails.resolveStatement();
		}
		else {
			bindingGroup.forEachBinding( (binding) -> {
				try {
					binding.getValueBinder().bind(
							statementDetails.resolveStatement(),
							binding.getValue(),
							binding.getPosition(),
							session
					);
				}
				catch (SQLException e) {
					throw session.getJdbcServices().getSqlExceptionHelper().convert(
							e,
							String.format(
									Locale.ROOT,
									"Unable to bind parameter #%s - %s",
									binding.getPosition(),
									binding.getValue()
							)
					);
				}
			} );
		}
	}

	@Override
	public void afterStatement(TableMapping mutatingTable) {
		final BindingGroup bindingGroup = bindingGroupMap.remove( mutatingTable.getTableName() );
		if ( bindingGroup == null ) {
			return;
		}

		bindingGroup.clear();
	}

	/**
	 * Access to {@link JdbcValueDescriptor} values
	 */
	@FunctionalInterface
	public interface JdbcValueDescriptorAccess {
		JdbcValueDescriptor resolveValueDescriptor(String tableName, String columnName, ParameterUsage usage);
	}
}
