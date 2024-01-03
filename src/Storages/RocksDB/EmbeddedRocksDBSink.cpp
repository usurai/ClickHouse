#include <Storages/RocksDB/EmbeddedRocksDBSink.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <IO/WriteBufferFromString.h>

#include <rocksdb/utilities/db_ttl.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

EmbeddedRocksDBSink::EmbeddedRocksDBSink(
    StorageEmbeddedRocksDB & storage_,
    const StorageMetadataPtr & metadata_snapshot_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
{
    std::vector<bool> is_pk(getHeader().columns());

    primary_key_pos.reserve(storage.primary_key.size());
    for (const auto& key_name : storage.primary_key) {
        primary_key_pos.push_back(getHeader().getPositionByName(key_name));
        is_pk[primary_key_pos.back()] = true;
    }

    non_primay_key_pos.reserve(is_pk.size() - primary_key_pos.size());
    for (size_t i = 0; i < is_pk.size(); ++i) {
        if (!is_pk[i])
            non_primay_key_pos.push_back(i);
    }

    serializations = getHeader().getSerializations();
}

void EmbeddedRocksDBSink::consume(Chunk chunk)
{
    auto rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();

    WriteBufferFromOwnString wb_key;
    WriteBufferFromOwnString wb_value;

    rocksdb::WriteBatch batch;
    rocksdb::Status status;
    for (size_t i = 0; i < rows; ++i)
    {
        wb_key.restart();
        wb_value.restart();

        for (const auto col : primary_key_pos)
            serializations[col]->serializeBinary(*columns[col], i, wb_key, {});

        for (const auto col : non_primay_key_pos)
            serializations[col]->serializeBinary(*columns[col], i, wb_value, {});

        status = batch.Put(wb_key.str(), wb_value.str());
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
    }

    status = storage.rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
}

}
