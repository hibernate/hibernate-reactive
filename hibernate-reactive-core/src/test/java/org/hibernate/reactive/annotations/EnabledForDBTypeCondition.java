/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
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

public class EnabledForDBTypeCondition implements ExecutionCondition {

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		List<EnabledFor> annotations = findRepeatableAnnotations( context.getElement(), EnabledFor.class );
		if ( !annotations.isEmpty() ) {
			return evaluateAnnotation( annotations );
		}
		return enabled( "No @EnabledFor condition resulting in 'disabled' execution encountered" );
	}

	private ConditionEvaluationResult evaluateAnnotation(Iterable<EnabledFor> values) {
		StringBuilder enabledList = new StringBuilder( "," );
		for ( EnabledFor annotation : values ) {
			for ( DBType enabledDbType : annotation.value() ) {
				if ( enabledDbType == dbType() ) {
					return enabled( annotation.reason() );
				}
				enabledList.append( ',' ).append( Arrays.toString( annotation.value() ) );
			}
		}
		return disabled( String.format(
				"%s does not match any db in the enabled list: %s",
				dbType(),
				enabledList.substring( 1 )
		) );
	}
}
