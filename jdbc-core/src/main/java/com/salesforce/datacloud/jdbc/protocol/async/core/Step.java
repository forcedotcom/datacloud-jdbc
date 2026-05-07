/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.async.core;

import java.util.Objects;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

/**
 * Sum-type result of one step of {@link AsyncIterator#next()}.
 *
 * <p>The async pump may need to ask its caller for cooperation before producing a value.
 * Concretely, follow-up gRPC calls (e.g. {@code getQueryInfo}, {@code getQueryResult}) must be
 * dispatched on the caller's thread so that gRPC {@link io.grpc.ClientInterceptor} {@code start}
 * callbacks observe the caller's {@link ThreadLocal} state. To make this possible without
 * threading an executor through the iterator chain, an iterator can return
 * {@link NeedDispatch}; the synchronous pump then runs the supplied thunk on the caller thread
 * and re-invokes {@code next()}.</p>
 *
 * <p>{@link Value} carries the next produced item. {@link Done} signals end-of-iteration.</p>
 *
 * <p>Construction is restricted to the static factories below — the nested subclasses' constructors
 * are private and {@link Step} itself has a private constructor, so {@code Value / Done / NeedDispatch}
 * is the closed set of cases consumers must handle. A consumer's {@code instanceof} chain should
 * cover all three and throw on anything else, so adding a fourth case (a deliberate change inside
 * this file) trips a runtime failure rather than silently being treated as end-of-iteration.</p>
 *
 * @param <T> element type produced by the iterator
 */
public abstract class Step<T> {

    private Step() {}

    /**
     * @return a step carrying the next produced item; {@code item} must be non-null
     */
    public static <T> Step<T> value(T item) {
        return new Value<>(Objects.requireNonNull(item, "Step.value item must be non-null"));
    }

    /**
     * @return a step signaling that iteration is complete
     */
    public static <T> Step<T> done() {
        return new Done<>();
    }

    /**
     * @return a step asking the synchronous pump to run {@code dispatch} on the caller thread,
     *         then re-invoke {@code next()}
     */
    public static <T> Step<T> needDispatch(Runnable dispatch) {
        return new NeedDispatch<>(Objects.requireNonNull(dispatch, "Step.needDispatch dispatch must be non-null"));
    }

    /**
     * Re-types a {@link NeedDispatch} from one element type to another. The wrapped
     * {@link Runnable} does not depend on the element type, so this is a safe upcast that
     * avoids reallocating a new {@code NeedDispatch} at every iterator boundary.
     */
    @SuppressWarnings("unchecked")
    public static <U> Step<U> retypeNeedDispatch(NeedDispatch<?> step) {
        return (Step<U>) step;
    }

    /** A produced item. */
    @lombok.Value
    @lombok.EqualsAndHashCode(callSuper = false)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Value<T> extends Step<T> {
        T item;
    }

    /** End-of-iteration sentinel. */
    @lombok.EqualsAndHashCode(callSuper = false)
    public static final class Done<T> extends Step<T> {
        private Done() {}
    }

    /**
     * Sentinel asking the synchronous pump to execute {@code dispatch} on the caller thread,
     * then re-invoke {@code next()} to continue iteration.
     */
    @lombok.Value
    @lombok.EqualsAndHashCode(callSuper = false)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class NeedDispatch<T> extends Step<T> {
        Runnable dispatch;
    }
}
