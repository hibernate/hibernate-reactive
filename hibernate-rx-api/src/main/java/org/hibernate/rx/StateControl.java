package org.hibernate.rx;

import java.util.concurrent.CompletionStage;

public interface StateControl {

	void clear();

	CompletionStage<Void> refresh(Object var1);

	CompletionStage<Void> flush();

}
