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

public class EnableForDBTypeCondition<A extends EnableFor> implements ExecutionCondition {

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		Optional<EnableForGroup> enableForGroup = findAnnotation( context.getElement(), EnableForGroup.class );
		if ( enableForGroup != null && enableForGroup.isPresent() ) {
			for ( EnableFor annotation : enableForGroup.get().value() ) {
				if ( annotation.value() == DatabaseConfiguration.dbType() ) {
					return ConditionEvaluationResult.enabled( annotation.reason() );
				}
			}
			return ConditionEvaluationResult.disabled( "" );
		}

		Optional<EnableFor> enableFor = findAnnotation( context.getElement(), EnableFor.class );
		if( enableFor != null && enableFor.isPresent() ) {
			DatabaseConfiguration.DBType type = enableFor.get().value();
			return type == DatabaseConfiguration.dbType() ?
				ConditionEvaluationResult.enabled( "" ) :
				ConditionEvaluationResult.disabled( "" );
		}
		return ConditionEvaluationResult.enabled( "" );
	}
}
