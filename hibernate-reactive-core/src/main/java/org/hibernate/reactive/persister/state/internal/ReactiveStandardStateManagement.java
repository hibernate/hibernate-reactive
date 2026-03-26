/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.state.internal;

/**
 * @see org.hibernate.persister.state.internal.StandardStateManagement
 */
public class ReactiveStandardStateManagement extends ReactiveAbstractStateManagement {
	public static final ReactiveStandardStateManagement INSTANCE = new ReactiveStandardStateManagement();
}
