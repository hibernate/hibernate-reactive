package org.hibernate.rx.persister.entity.impl;

/**
 * Internal interface to enable double dispatch between
 * the {@link RxEntityPersister} and its
 * {@link org.hibernate.persister.entity.EntityPersister}.
 *
 * Note that {@link RxEntityPersister}s are stateless
 * decorators and aren't a good place to hang an
 * {@link RxIdentifierGenerator}.
 */
public interface RxGeneratedIdentifierPersister {
	RxIdentifierGenerator getRxIdentifierGenerator();
}
