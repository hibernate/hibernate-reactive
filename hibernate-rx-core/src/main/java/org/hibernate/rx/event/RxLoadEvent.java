package org.hibernate.rx.event;

import java.io.Serializable;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.rx.RxSession;

public class RxLoadEvent extends LoadEvent {

	public RxLoadEvent(Serializable entityId, Object instanceToLoad, EventSource source, RxSession rxSession) {
		super( entityId, instanceToLoad, source );
	}

	public RxLoadEvent(
			Serializable entityId,
			String entityClassName,
			LockMode lockMode,
			EventSource source, RxSession rxSession) {
		super( entityId, entityClassName, lockMode, source );
	}

	public RxLoadEvent(
			Serializable entityId,
			String entityClassName,
			LockOptions lockOptions,
			EventSource source, RxSession rxSession) {
		super( entityId, entityClassName, lockOptions, source );
	}

	public RxLoadEvent(
			Serializable entityId,
			String entityClassName,
			boolean isAssociationFetch,
			EventSource source, RxSession rxSession) {
		super( entityId, entityClassName, isAssociationFetch, source );
	}
}
