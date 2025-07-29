## Adaptive Query Pattern

This below diagram represents the `Adaptive` transfer mode flow which works in two phases:

**Initial Phase**: The `ExecuteQuery` call returns up to one result chunk synchronously along with query status information, allowing immediate data access while the query may still be running.

**Async Fetching Phase**: Once the initial stream ends, the machine decides between `GetQueryResult` (when it knows specific chunks are available) and `GetQueryInfo` (when it needs to check for newly available chunks). This avoids unnecessary polling while ensuring all data is eventually retrieved.

This approach provides faster time-to-first-result than pure async mode while avoiding the timeout risks of sync mode for large result sets. The entire state machine is exposed to client code as an `Iterator<QueryResult>`, where each call to `next()` may trigger state transitions and API calls as needed.

```mermaid
stateDiagram-v2
    direction TB
    [*] --> ExecuteQuery
    
    ExecuteQuery --> ProcessExecuteQueryStream: Stream of ExecuteQueryResponse
    
    ProcessExecuteQueryStream --> ProcessQueryInfo: Contains QueryInfo
    ProcessExecuteQueryStream --> ProcessQueryResult: Contains QueryResult
    ProcessExecuteQueryStream --> StreamEnded: No more messages
    
    ProcessQueryInfo --> UpdateQueryContext: Update known chunks/status
    ProcessQueryResult --> EmitResultData: Emit data to client
    
    UpdateQueryContext --> ProcessExecuteQueryStream: Continue processing stream
    EmitResultData --> ProcessExecuteQueryStream: Continue processing stream
    
    StreamEnded --> CheckForMoreData: Check if more chunks available
    
    CheckForMoreData --> GetQueryResult: Known chunks remaining
    CheckForMoreData --> GetQueryInfo: Unknown if more chunks exist
    CheckForMoreData --> [*]: All data consumed
    
    GetQueryInfo --> ProcessQueryInfoResponse: Response received
    ProcessQueryInfoResponse --> UpdateQueryContext2: Update known chunks/status
    UpdateQueryContext2 --> CheckForMoreData: Re-evaluate what to do next
    
    GetQueryResult --> ProcessQueryResultResponse: Response received  
    ProcessQueryResultResponse --> EmitResultData2: Emit data to client
    EmitResultData2 --> CheckForMoreData: Re-evaluate what to do next

    note right of ExecuteQuery : gRPC call with ADAPTIVE mode
    note right of GetQueryInfo : gRPC call to check status
    note right of GetQueryResult : gRPC call to fetch specific chunk
```
