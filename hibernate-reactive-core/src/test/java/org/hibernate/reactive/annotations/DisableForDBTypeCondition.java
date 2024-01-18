/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.annotations;

import java.util.Optional;

import org.hibernate.reactive.containers.DatabaseConfiguration;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;

class DisableForDBTypeCondition<A extends DisableFor> implements ExecutionCondition {

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		Optional<DisableForGroup> disabledForGroup = findAnnotation( context.getElement(), DisableForGroup.class );
		if ( disabledForGroup != null && disabledForGroup.isPresent() ) {
			for ( DisableFor annotation : disabledForGroup.get().value() ) {
				if ( annotation.value() == DatabaseConfiguration.dbType() ) {
					return ConditionEvaluationResult.disabled( annotation.reason() );
				}
			}
			return ConditionEvaluationResult.enabled( "" );
		}

		Optional<DisableFor> disabledFor = findAnnotation( context.getElement(), DisableFor.class );
		if ( disabledFor.isPresent() ) {
			DatabaseConfiguration.DBType type = disabledFor.get().value();
			return type == DatabaseConfiguration.dbType() ?
				ConditionEvaluationResult.disabled( disabledFor.get().reason() ) :
				ConditionEvaluationResult.enabled( "" );
		}
		return ConditionEvaluationResult.enabled( "" );
	}
}
