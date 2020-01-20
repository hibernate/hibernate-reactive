package org.hibernate.rx.event.impl;

import org.hibernate.event.service.spi.DuplicationStrategy;

public class ReplacementDuplicationStrategy implements DuplicationStrategy {

	public static final DuplicationStrategy INSTANCE = new ReplacementDuplicationStrategy();

	private ReplacementDuplicationStrategy() {}

	@Override
	public boolean areMatch(Object listener, Object original) {
		return listener.getClass().getName().startsWith("org.hibernate.rx.event.impl");
	}

	@Override
	public Action getAction() {
		return Action.REPLACE_ORIGINAL;
	}
}
