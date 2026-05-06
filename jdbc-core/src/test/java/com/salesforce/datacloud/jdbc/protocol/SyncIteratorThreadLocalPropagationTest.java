/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.core.InterceptedHyperTestBase;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Value;
import lombok.val;
import org.junit.jupiter.api.Test;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryStatus;

/**
 * Investigates whether a {@link ThreadLocal} set on the caller thread is visible to a gRPC
 * {@link ClientInterceptor} on every RPC the {@code SyncIteratorAdapter} fires, including the
 * follow-on {@code getQueryInfo} and {@code getQueryResult} calls that happen lazily as the
 * caller drains the result.
 *
 * <p>The scenario: caller has a {@link ThreadLocal} value (e.g. an audit token, request id, or
 * routing hint) and an interceptor that reads it. The caller does not want the value moved
 * into the interceptor's constructor — it must reflect what is set on the calling thread at
 * the moment of each RPC.</p>
 *
 * <p>Each test asserts on which thread the interceptor's {@code start(...)} actually fired
 * AND what value the {@code ThreadLocal} held at that moment, for every RPC the iterator drove.
 * This pins down whether the protocol layer's async machinery preserves the caller's
 * {@code ThreadLocal} through all transitive gRPC calls.</p>
 */
public class SyncIteratorThreadLocalPropagationTest extends InterceptedHyperTestBase {

    private static final String QUERY = "SELECT 1 thread_local_test";
    private static final String QUERY_ID = "tl-test-query-1";

    private static final ThreadLocal<String> CALLER_TL = new ThreadLocal<>();

    /**
     * Interceptor that — at the moment {@code start} fires — snapshots the current thread name
     * and the value of the caller's {@link ThreadLocal}. This is the realistic shape of an
     * interceptor that "depends on the value of the thread local" (e.g. injecting an
     * audit/request header derived from caller state).
     */
    @Value
    static class Observation {
        String method;
        String threadName;
        String threadLocalValue;
    }

    static final class RecordingInterceptor implements ClientInterceptor {
        final List<Observation> observations = new CopyOnWriteArrayList<>();

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions options, Channel next) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, options)) {
                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    observations.add(new Observation(
                            method.getFullMethodName(), Thread.currentThread().getName(), CALLER_TL.get()));
                    super.start(responseListener, headers);
                }
            };
        }
    }

    @Test
    public void singleRpcOnly_executeQueryInlineFinished_threadLocalIsSeen() {
        // Scenario: query finishes inline on the executeQuery stream, no follow-up RPCs.
        // executeQuery is started synchronously from QueryResultIterator.of(...) on the
        // caller thread, so the ThreadLocal must be visible to the interceptor.
        val recorder = new RecordingInterceptor();
        val stub = getInterceptedStub().withInterceptors(recorder);
        val params = setupExecuteQuery(
                QUERY_ID,
                QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(QUERY_ID, QueryStatus.CompletionStatus.FINISHED, 1));

        val callerThread = Thread.currentThread().getName();
        CALLER_TL.set("token-A");
        try {
            val iterator = QueryResultIterator.of(stub, params);
            // Drain the iterator end-to-end on the caller thread.
            while (iterator.hasNext()) {
                iterator.next();
            }
        } finally {
            CALLER_TL.remove();
        }

        assertThat(recorder.observations).hasSize(1);
        val executeObs = recorder.observations.get(0);
        assertThat(executeObs.getMethod()).endsWith("/ExecuteQuery");
        assertThat(executeObs.getThreadName()).isEqualTo(callerThread);
        assertThat(executeObs.getThreadLocalValue()).isEqualTo("token-A");
    }

    @Test
    public void multiRpc_executeQuery_thenGetQueryInfo_thenGetQueryResult_threadLocalIsSeenOnEveryCall() {
        // Scenario: the iterator must do all three RPCs in sequence:
        //   1. executeQuery returns RUNNING — no inline data
        //   2. getQueryInfo polls and returns FINISHED with a chunk count of 2
        //   3. getQueryResult fetches chunk 1
        // Calls 2 and 3 are kicked off from inside thenCompose() callbacks on the gRPC executor
        // thread, NOT from the caller thread. If the protocol layer doesn't preserve the
        // caller's ThreadLocal, those calls will see a null/wrong value.
        val recorder = new RecordingInterceptor();
        val stub = getInterceptedStub().withInterceptors(recorder);
        val params = setupExecuteQuery(
                QUERY_ID,
                QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(QUERY_ID, QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED, 1));
        setupGetQueryInfo(QUERY_ID, QueryStatus.CompletionStatus.FINISHED, 2);
        setupGetQueryResult(QUERY_ID, 1, 1, Collections.emptyList());

        val callerThread = Thread.currentThread().getName();
        CALLER_TL.set("token-B");
        try {
            val iterator = QueryResultIterator.of(stub, params);
            while (iterator.hasNext()) {
                iterator.next();
            }
        } finally {
            CALLER_TL.remove();
        }

        // Sanity: we drove all three call types.
        assertThat(recorder.observations)
                .extracting(Observation::getMethod)
                .anyMatch(m -> m.endsWith("/ExecuteQuery"))
                .anyMatch(m -> m.endsWith("/GetQueryInfo"))
                .anyMatch(m -> m.endsWith("/GetQueryResult"));

        // The first RPC (ExecuteQuery) is started synchronously from QueryResultIterator.of()
        // and therefore runs on the caller thread.
        val executeObs = recorder.observations.stream()
                .filter(o -> o.getMethod().endsWith("/ExecuteQuery"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("ExecuteQuery RPC was not observed"));
        assertThat(executeObs.getThreadName())
                .as("ExecuteQuery is started synchronously from of(), so it runs on the caller thread")
                .isEqualTo(callerThread);
        assertThat(executeObs.getThreadLocalValue())
                .as("ExecuteQuery interceptor should observe the caller's thread-local")
                .isEqualTo("token-B");

        // The follow-up RPCs are dispatched from inside thenCompose() callbacks. The protocol
        // layer's contract is that the dispatch thunk runs on the caller thread, so the
        // interceptor's start(...) must fire on that same thread AND see the caller's
        // ThreadLocal. Asserting both pins the contract end-to-end — a future implementation
        // that snapshots the value but dispatches on a worker thread would still satisfy the
        // value check but break the thread-identity invariant.
        for (Observation obs : recorder.observations) {
            if (obs.getMethod().endsWith("/ExecuteQuery")) {
                continue;
            }
            assertThat(obs.getThreadName())
                    .as("Follow-up RPC %s should fire on the caller thread, not a gRPC executor", obs.getMethod())
                    .isEqualTo(callerThread);
            assertThat(obs.getThreadLocalValue())
                    .as(
                            "Interceptor observation for %s on thread '%s' — caller thread was '%s'."
                                    + " A null here means the caller's ThreadLocal did NOT propagate"
                                    + " into the gRPC executor thread that fired this RPC.",
                            obs.getMethod(), obs.getThreadName(), callerThread)
                    .isEqualTo("token-B");
        }
    }

    @Test
    public void multiRpc_threadLocalChangedBetweenHasNextCalls_isReflectedOnNextRpc() {
        // Scenario: caller mutates the ThreadLocal between drains. The protocol layer should
        // reflect the *current* caller-thread value on RPCs that are dispatched while the
        // caller is sitting in hasNext()/next() — not a snapshotted value.
        //
        // Concretely: drain the inline ExecuteQuery first under "token-pre", then mutate the
        // ThreadLocal to "token-post" before triggering the GetQueryInfo + GetQueryResult RPCs.
        // The first inline result is delivered from executeQuery, which is started on the caller
        // thread under "token-pre". After draining that, the iterator must do GetQueryInfo +
        // GetQueryResult to get the next chunk. We probe whether those see "token-post" or not.
        val recorder = new RecordingInterceptor();
        val stub = getInterceptedStub().withInterceptors(recorder);
        val params = setupExecuteQuery(
                QUERY_ID,
                QUERY,
                QueryParam.TransferMode.ADAPTIVE,
                executeQueryResponse(QUERY_ID, QueryStatus.CompletionStatus.RUNNING_OR_UNSPECIFIED, 1));
        setupGetQueryInfo(QUERY_ID, QueryStatus.CompletionStatus.FINISHED, 2);
        setupGetQueryResult(QUERY_ID, 1, 1, Collections.emptyList());

        val callerThread = Thread.currentThread().getName();
        CALLER_TL.set("token-pre");
        QueryResultIterator iterator;
        try {
            iterator = QueryResultIterator.of(stub, params);
            // Force ExecuteQuery to be observed under token-pre. hasNext() will block until
            // the executeQuery stream is drained and chunk fetching begins.
        } finally {
            // Simulate the caller swapping the value before the next blocking probe.
        }

        // ExecuteQuery should already have been recorded under token-pre.
        val executeObs = recorder.observations.stream()
                .filter(o -> o.getMethod().endsWith("/ExecuteQuery"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("ExecuteQuery RPC was not observed"));
        assertThat(executeObs.getThreadLocalValue()).isEqualTo("token-pre");

        // Now flip the ThreadLocal and drive the next round — this drives GetQueryInfo and
        // GetQueryResult, both started from inside thenCompose callbacks.
        CALLER_TL.set("token-post");
        try {
            while (iterator.hasNext()) {
                iterator.next();
            }
        } finally {
            CALLER_TL.remove();
        }

        // For each follow-on RPC, assert what the interceptor saw. If propagation is broken,
        // the observed value will be null (interceptor ran on a gRPC executor thread which
        // never had the caller's ThreadLocal set) or "token-pre" (snapshotted somewhere upstream).
        // The contract we want is: the value at the moment the RPC was dispatched, which from
        // the caller's perspective is "token-post".
        for (Observation obs : recorder.observations) {
            if (obs.getMethod().endsWith("/ExecuteQuery")) {
                continue;
            }
            assertThat(obs.getThreadName())
                    .as("Follow-up RPC %s should fire on the caller thread, not a gRPC executor", obs.getMethod())
                    .isEqualTo(callerThread);
            assertThat(obs.getThreadLocalValue())
                    .as(
                            "Follow-up RPC %s on thread '%s' should observe the caller's"
                                    + " *current* ThreadLocal value at dispatch time.",
                            obs.getMethod(), obs.getThreadName())
                    .isEqualTo("token-post");
        }
    }
}
