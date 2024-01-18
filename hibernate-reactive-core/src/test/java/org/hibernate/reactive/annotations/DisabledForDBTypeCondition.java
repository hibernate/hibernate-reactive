/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.annotations;

import java.util.Arrays;
import java.util.List;

import org.hibernate.reactive.containers.DatabaseConfiguration.DBType;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;
import static org.junit.platform.commons.util.AnnotationUtils.findRepeatableAnnotations;

class DisabledForDBTypeCondition implements ExecutionCondition {

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		List<DisabledFor> annotations = findRepeatableAnnotations( context.getElement(), DisabledFor.class );
		if ( !annotations.isEmpty() ) {
			return evaluateAnnotation( annotations );
		}
		return enabled( "No @DisabledFor condition resulting in 'disabled' execution encountered" );
	}

	private ConditionEvaluationResult evaluateAnnotation(Iterable<DisabledFor> values) {
		StringBuilder disabledList = new StringBuilder( "," );
		for ( DisabledFor annotation : values ) {
			for ( DBType enabledDbType : annotation.value() ) {
				if ( enabledDbType == dbType() ) {
					return disabled( annotation.reason() );
				}
				disabledList.append( ',' ).append( Arrays.toString( annotation.value() ) );
			}
		}
		return enabled( String.format(
				"%s does not match any db in the disabled list: %s",
				dbType(),
				disabledList.substring( 1 )
		) );
	}
}
