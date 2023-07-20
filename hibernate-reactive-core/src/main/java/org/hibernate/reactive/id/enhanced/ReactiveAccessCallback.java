/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.enhanced;

import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.id.enhanced.AccessCallback;

public interface ReactiveAccessCallback extends AccessCallback {
	InternalStage<IntegralDataTypeHolder> getNextReactiveValue();
}
