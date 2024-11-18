/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mapping.internal;

import org.hibernate.MappingException;
import org.hibernate.mapping.KeyValue;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Value;
import org.hibernate.type.Type;

public class ReactiveKeyValueWrapper extends SimpleValue implements KeyValue {

	protected ReactiveKeyValueWrapper(SimpleValue original) {
		super( original );
	}

	@Override
	public Type getType() throws MappingException {
		return null;
	}

	@Override
	public Value copy() {
		return null;
	}
}
