## Chosen for development
- TODO: measure synchronization with atomics

## Features
- TODO: add committing offsets
- TODO: add adding topics

## Improvements
- TODO: add abstracted mechanism for correlating requests and responses
- TODO: add success/error responses for consume and produce requests or create partitions implicitly
- TODO: test adding/removing producers/consumers
- TODO: Also close producers and consumers when closing a client
- TODO: try what happens on the client, when the server side closes the call
- TODO: add minimum number messages or max time logic
- TODO: analyze where we have back pressure and where not
- TODO: make sending acks non blocking for the partition produce handler but still ordered per client and partition
- TODO: you can make notifying per partition consume handler about new messages non blocking
- TODO: you can make notifying per consumer consume handler about new messages non blocking

## Experiments