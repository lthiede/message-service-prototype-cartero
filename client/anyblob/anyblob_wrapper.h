#ifndef ANYBLOBWRAPPER_H
#define ANYBLOBWRAPPER_H

#include <stddef.h>
#include <stdint.h>

// unsigned defaultChunkSize = 64u * 1024;

/// The number of submissions in queue (per thread)
// uint64_t defaultSubmissionPerCore = 1 << 10;

// uint64_t defaultReuse = 0;

// uint64_t defaultHttps = 0;

// char defaultKeyId[] = "";

// char defaultKeyFile[] = "";


// uint64_t defaultRangeStart = 0;

// uint64_t defaultRangeEnd = 0;

// uint8_t* defaultResult = NULL;

// uint64_t defaultCapacity = 0;

// uint64_t defaultTraceId = 0;

#ifdef __cplusplus
#include "cloud/provider.hpp"
#include "network/tasked_send_receiver.hpp"
#include "network/message_result.hpp"
#include "network/transaction.hpp"

// anyblob::network::TaskedSendReceiverHandle* defaultSendReceiverHandle = NULL;

extern "C" {

extern int hello();

extern anyblob::network::TaskedSendReceiverGroup* call_TaskedSendReceiverGroup_Constructor(unsigned chunkSize, uint64_t submissions, uint64_t reuse);

extern anyblob::network::TaskedSendReceiverHandle* call_Group_getHandle(anyblob::network::TaskedSendReceiverGroup* group);

extern std::unique_ptr<anyblob::cloud::Provider>* call_makeProvider(char filepath[], uint64_t https, char keyId[], char keyFile[], anyblob::network::TaskedSendReceiverHandle* sendReceiverHandle);

extern void call_Provider_initCache(std::unique_ptr<anyblob::cloud::Provider>* provider, anyblob::network::TaskedSendReceiverHandle* sendReceiverHandle);

extern anyblob::network::Config* call_Provider_getConfig(std::unique_ptr<anyblob::cloud::Provider>* provider, anyblob::network::TaskedSendReceiverHandle* sendReceiverHandle);

extern anyblob::network::Transaction* call_Transaction_Constructor(std::unique_ptr<anyblob::cloud::Provider>* provider);

extern uint64_t call_Transaction_getObjectRequest(anyblob::network::Transaction* txn, char remotePath[], uint64_t rangeStart, uint64_t rangeEnd, uint8_t* result, uint64_t capacity, uint64_t traceId);

extern void call_Transaction_processSync(anyblob::network::Transaction* txn, anyblob::network::TaskedSendReceiverHandle* sendReceiverHandle);

// extern anyblob::network::Transaction::Iterator* call_Transaction_begin(anyblob::network::Transaction* txn);

// extern anyblob::network::Transaction::Iterator* call_Transaction_end(anyblob::network::Transaction* txn);

// extern uint8_t* call_Iterator_getData(anyblob::network::MessageResult* iterator);

// extern uint64_t call_Iterator_getOffset(anyblob::network::MessageResult* iterator);

// extern uint64_t call_Iterator_getSize(anyblob::network::MessageResult* iterator);

} // extern "C"
#else

struct TaskedSendReceiverHandle* defaultSendReceiverHandle = NULL;

extern int hello();

extern struct TaskedSendReceiverGroup* call_TaskedSendReceiverGroup_Constructor(unsigned chunkSize, uint64_t submissions, uint64_t reuse);

extern struct TaskedSendReceiverHandle* call_Group_getHandle(struct TaskedSendReceiverGroup* group);

extern struct Provider* call_makeProvider(char filepath[], uint64_t https, char keyId[], char keyFile[], struct TaskedSendReceiverHandle* sendReceiverHandle);

extern void call_Provider_initCache(struct Provider* provider, struct TaskedSendReceiverHandle* sendReceiverHandle);

extern struct Config* call_Provider_getConfig(struct Provider* provider, struct TaskedSendReceiverHandle* sendReceiverHandle);

extern struct Transaction* call_Transaction_Constructor(struct Provider* provider);

extern uint64_t call_Transaction_getObjectRequest(struct Transcation* txn, char remotePath[], uint64_t rangeStart, uint64_t rangeEnd, uint8_t* result, uint64_t capacity, uint64_t traceId);

extern void call_Transaction_processSync(struct Transaction* txn, struct TaskedSendReceiverHandle* sendReceiverHandle);

// extern struct Iterator* call_Transaction_begin(struct Transaction* txn);

// extern struct Iterator* call_Transaction_end(struct Transcation* txn);

// extern uint8_t* call_Iterator_getData(struct Iterator* iterator);

// extern uint64_t call_Iterator_getOffset(struct Iterator* iterator);

// extern uint64_t call_Iterator_getSize(struct Iterator* iterator);

#endif

#endif /*ANYBLOBWRAPPER_H*/