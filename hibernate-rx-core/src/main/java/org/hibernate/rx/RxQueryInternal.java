package org.hibernate.rx;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.query.Query;

public interface RxQueryInternal<R> extends Query<R> {

	CompletionStage<R> getRxSingleResult();

	CompletionStage<List<R>> getRxResultList();

	CompletionStage<List<R>> rxList();
}
