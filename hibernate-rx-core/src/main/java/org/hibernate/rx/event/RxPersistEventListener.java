package org.hibernate.rx.event;

import java.io.Serializable;
import java.util.Map;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.PersistEventListener;

public interface RxPersistEventListener extends Serializable, PersistEventListener {

	void onPersist(RxPersistEvent event) throws HibernateException;

	void onPersist(RxPersistEvent event, Map<?, ?> createdAlready)
			throws HibernateException;

}