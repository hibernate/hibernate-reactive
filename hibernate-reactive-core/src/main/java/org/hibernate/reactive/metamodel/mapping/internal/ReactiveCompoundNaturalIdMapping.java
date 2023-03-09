/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.metamodel.mapping.internal;

import org.hibernate.loader.ast.spi.MultiNaturalIdLoader;
import org.hibernate.loader.ast.spi.NaturalIdLoader;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.SingularAttributeMapping;
import org.hibernate.metamodel.mapping.internal.CompoundNaturalIdMapping;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationProcess;
import org.hibernate.reactive.loader.ast.internal.ReactiveCompoundNaturalIdLoader;
import org.hibernate.reactive.loader.ast.internal.ReactiveMultiNaturalIdLoaderStandard;

import java.util.List;

public class ReactiveCompoundNaturalIdMapping extends CompoundNaturalIdMapping {
    public ReactiveCompoundNaturalIdMapping(EntityMappingType declaringType,
                                            List<SingularAttributeMapping> attributes,
                                            MappingModelCreationProcess creationProcess ) {
        super( declaringType, attributes, creationProcess );
    }

    @Override
    public NaturalIdLoader<?> makeLoader(EntityMappingType entityDescriptor) {
        return new ReactiveCompoundNaturalIdLoader<>( this, entityDescriptor );
    }

    @Override
    public MultiNaturalIdLoader<?> makeMultiLoader(EntityMappingType entityDescriptor) {
        return new ReactiveMultiNaturalIdLoaderStandard<>( entityDescriptor );
    }
}
