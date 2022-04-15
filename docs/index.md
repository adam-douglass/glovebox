## Configuration

> TODO

## API Endpoints

#### `GET` `/status/<queue_name>`
Read the status of an existing queue. Response is JSON on success.

#### `WS` `/connect/`
Connect to the server to send or fetch messages. If the `Binary` header is set to `true` the messages from the server will be bincoded, otherwise text encoded JSON is used. Regardless of the header, text/json and binary/bincode messages are accepted based on websocket message type. 

When using JSON encoding, messages will be stored as a string, if the content of the message can't be utf8 encoded, an array of bytes is sent instead. Messages from the server will always follow this convention regardless of how the message was encoded by the sender. Clients may always send messages in either format.

### Creating Queues

> TODO

### Sending Messages

To send a message via JSON send an object with the following fields:

| Key  | Description |
| ---- | ----------- |
| `type` | Must be the string `post` to send messages |
| `queue` | Name of queue to write message to |
| `message` | Message body |
| `priority` | A message priority, unsigned 32 bit integer expected. |
| `label` | A unique value the server will include in any response messages to indicate the response relates to this message. Unsigned 64 bit integer expected. |
| `notify` | A list of processing stages to send responses at. |

The values of `notify` result in the sender getting confirmation messages at the following times:

| Stage | Description | 
| -- | -- | 
| `ready`  | enqueued in memory | 
| `write`  | written to disk, may still be cached |
| `sync`   | synced to disk |
| `assign` | assigned to a consuming client |
| `finish` | message completed by client |
| `retry`  | assigned client timed out |
| `drop`   | retry limit reached |

Which notification messages are available depends on whether the consumer is using the `pop` or `fetch` commands. 

When a message is retrieved by `fetch` all notification values will be available.

When a message is popped, it is assumed completed as soon as it is assigned to a consumer. Retries, drops, and assign messages are never sent for popped messages. The only notification that is guaranteed to work the same as with fetch is `finish`. `ready` is also guaranteed to always receive a response, but some of those responses may be tagged as `finish`. This is due to a "fast pop" optimization where a pending pop will be satisfied by an incoming message without that message being enqueued in memory or on disk. If there are no pending pop commands, `ready`, `write`, and `sync` will occur normally as the message is enqueued.

### Receiving Messages

There are two approaches to fetching messages: 
 - Popping which is faster, but gives up some other features and safety guarantees in return for speed.
 - Fetching which gets messages via a handshake that guarantees message delivery and processing at the cost of complexity and time limits on processing.

#### Popping messages

To pop a message via JSON send an object with the following fields:

| Key  | Description |
| ---- | ----------- |
| `type` | Must be the string `pop` |
| `queue` | Name of queue to pop from |
| `timeout` | Optional. If set is the number of seconds to wait for a message if the queue is empty. |
| `label` | A unique value the server will include in any response messages to indicate the response relates to this message. Unsigned 64 bit integer expected. |

#### Fetching messages

To fetch a message via JSON send an object with the following fields:

| Key  | Description |
| ---- | ----------- |
| `type` | Must be the string `fetch` |
| `queue` | Name of queue to read from |
| `block_timeout` | The number of seconds to wait for a message if the queue is empty. |
| `work_timeout` | The number of seconds to wait for a finish command. |
| `sync` | How persisted should the assignment of the message be before it is sent to the client, options are `ready`, `write`, or `sync` |
| `label` | A unique value the server will include in any response messages to indicate the response relates to this message. Unsigned 64 bit integer expected. |

Once a message has been fetched, its processing can be confirmed by sending the following message:

| Key  | Description |
| ---- | ----------- |
| `type` | Must be the string `finish` |
| `queue` | Name of queue the finished message was taken from |
| `sequence` | Sequence number attached to the message |
| `shard` | Shard number attached to the message |
| `label` | Optional. A unique value the server will include in any response messages to indicate the response relates to this message. Unsigned 64 bit integer expected. |
| `response` | Optional. Same as `sync` in the fetch command, but for an acknowledgement that the finished state of the message has been persisted. |


### Server Response Formats

Messages from the server take the following forms:

#### Hello

A hello message is sent to every new connected websocket.

| Key  | Description |
| ---- | ----------- |
| `type` | Always the string `hello` |
| `id` | Client id. Used for reconnection feature. |

#### Notice

A notice message is the formatting for any message lifecycle notifications from the server.

| Key  | Description |
| ---- | ----------- |
| `type` | Always the string `notice` |
| `label` | Label value used in message this notice relates to |
| `notice` | Which lifecycle stage this message is for |

#### Message

A message from a queue being sent to a consumer.

| Key  | Description |
| ---- | ----------- |
| `type` | Always the string `message` |
| `label` | Label value used in message this notice relates to |
| `shard` | Integral value identifying message on the server |
| `sequence` | Integral value identifying message on the server |
| `body` | Message body |
| `finished` | boolean flag set to true if the server considers this message processed and does not expect a finish message in response |

#### No Message

A message sent to inform that a pop or fetch command has timed out with no available messages.

| Key  | Description |
| ---- | ----------- |
| `type` | Always the string `nomessage` |
| `label` | Label value used in message this notice relates to |

#### Error

A message to notify the server has encountered an error with their request.

| Key  | Description |
| ---- | ----------- |
| `type` | Always the string `error` |
| `label` | Label value used in message this error relates to |
| `code` | Error code |
| `key` | A key string related to identifying the cause of the error |


| Error codes | Description |
| ---- | ---- |
| NoObject | The request tried to interact with an object that doesn't exist on the server |
| ObjectAlreadyExists | The request tried to create an object with a name already in use |
| PermissionDenied | The request was not permitted |


