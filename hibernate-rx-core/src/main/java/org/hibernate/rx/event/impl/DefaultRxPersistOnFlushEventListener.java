package org.hibernate.rx.event.impl;

import org.hibernate.engine.spi.CascadingAction;
import org.hibernate.engine.spi.CascadingActions;

public class DefaultRxPersistOnFlushEventListener extends DefaultRxPersistEventListener {
	@Override
	protected CascadingAction getCascadeAction() {
		return CascadingActions.PERSIST_ON_FLUSH;
	}
}