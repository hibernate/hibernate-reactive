/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import org.hibernate.loader.ast.internal.MultiNaturalIdLoaderStandard;
import org.hibernate.metamodel.mapping.EntityMappingType;

public class ReactiveMultiNaturalIdLoaderStandard<T> extends MultiNaturalIdLoaderStandard {
    public ReactiveMultiNaturalIdLoaderStandard(EntityMappingType entityDescriptor) {
        super(entityDescriptor);
    }
}
