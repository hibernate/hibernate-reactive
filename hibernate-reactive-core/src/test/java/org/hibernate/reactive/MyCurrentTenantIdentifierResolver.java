/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.context.spi.CurrentTenantIdentifierResolver;

public class MyCurrentTenantIdentifierResolver implements CurrentTenantIdentifierResolver {
    @Override
    public String resolveCurrentTenantIdentifier() {
        return "hello";
    }

    @Override
    public boolean validateExistingCurrentSessions() {
        return false;
    }
}
