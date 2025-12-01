/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.delegation;

import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.mutiny.delegation.MutinyStatelessSessionDelegator;

@SuppressWarnings("unused")
class ConcreteStatelessSessionDelegator extends MutinyStatelessSessionDelegator {
    @Override
    public Mutiny.StatelessSession delegate() {
        throw new UnsupportedOperationException();
    }
}
