#include "anyblob_wrapper.h"

int hello() {
    return 2342359;
}

anyblob::network::TaskedSendReceiverGroup* call_TaskedSendReceiverGroup_Constructor(unsigned chunkSize, uint64_t submissions, uint64_t reuse) {
    anyblob::network::TaskedSendReceiverGroup* group = new anyblob::network::TaskedSendReceiverGroup(chunkSize, submissions * std::thread::hardware_concurrency(), reuse);
    return group;
    // group.setConcurrentRequests with good default values
}

anyblob::network::TaskedSendReceiverHandle* call_Group_getHandle(anyblob::network::TaskedSendReceiverGroup* group) {
    anyblob::network::TaskedSendReceiverHandle* handle = new anyblob::network::TaskedSendReceiverHandle(std::move(group->getHandle()));
    return handle;
}

std::unique_ptr<anyblob::cloud::Provider>* call_makeProvider(char filepath[], uint64_t https, char keyId[], char keyFile[], anyblob::network::TaskedSendReceiverHandle* sendReceiverHandle) {
    if (sendReceiverHandle == NULL) {
        sendReceiverHandle = nullptr;
    }
    std::unique_ptr<anyblob::cloud::Provider>* provider = new std::unique_ptr<anyblob::cloud::Provider>(std::move(anyblob::cloud::Provider::makeProvider(filepath, https, keyId, keyFile, sendReceiverHandle)));
    return provider;
}

void call_Provider_initCache(std::unique_ptr<anyblob::cloud::Provider>* provider, anyblob::network::TaskedSendReceiverHandle* sendReceiverHandle) {
    (*provider)->initCache(*sendReceiverHandle);
}

anyblob::network::Config* call_Provider_getConfig(std::unique_ptr<anyblob::cloud::Provider>* provider, anyblob::network::TaskedSendReceiverHandle* sendReceiverHandle) {
    anyblob::network::Config* config = new anyblob::network::Config(std::move((*provider)->getConfig(*sendReceiverHandle)));
    return config;
}

anyblob::network::Transaction* call_Transaction_Constructor(std::unique_ptr<anyblob::cloud::Provider>* provider) {
    anyblob::network::Transaction* txn = new anyblob::network::Transaction((*provider).get());
    return txn;
}

uint64_t call_Transaction_getObjectRequest(anyblob::network::Transaction* txn, char remotePath[], uint64_t rangeStart, uint64_t rangeEnd, uint8_t* result, uint64_t capacity, uint64_t traceId) {
    if (result == NULL) {
        result = nullptr;
    }
    std::pair<uint64_t, uint64_t> range = {rangeStart, rangeEnd};
    return txn->getObjectRequest(remotePath, range, result, capacity, traceId);
}

void call_Transaction_processSync(anyblob::network::Transaction* txn, anyblob::network::TaskedSendReceiverHandle* sendReceiverHandle) {
    txn->processSync(*sendReceiverHandle);
}

// anyblob::network::Transaction::Iterator* call_Transaction_begin(anyblob::network::Transaction* txn) {
//     anyblob::network::Transaction::Iterator it = std::move(txn->begin());
//     return it;
// }

// anyblob::network::Transaction::Iterator* call_Transaction_end(anyblob::network::Transaction* txn) {
//     return &txn->end();
// }

// uint8_t* call_Iterator_getData(anyblob::network::MessageResult* iterator) {
//     return iterator->getData();
// }

// uint64_t call_Iterator_getOffset(anyblob::network::MessageResult* iterator) {
//     return iterator->getOffset();
// }

// uint64_t call_Iterator_getSize(anyblob::network::MessageResult* iterator) {
//     return iterator->getSize();
// }
