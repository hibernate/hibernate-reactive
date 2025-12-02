/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.delegation;

import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.mutiny.delegation.MutinySessionDelegator;

@SuppressWarnings("unused")
class ConcreteSessionDelegator extends MutinySessionDelegator {
    @Override
    public Mutiny.Session delegate() {
        throw new UnsupportedOperationException();
    }
}
