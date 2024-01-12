#pragma once

#include <Storages/SelectQueryInfo.h>

#include <Interpreters/PreparedSets.h>
#include <Core/Field.h>

#include <IO/ReadBufferFromString.h>

namespace DB
{

using FieldVectorPtr = std::shared_ptr<FieldVector>;
using FieldSet= std::set<Field>;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

/** Retrieve from the query a condition of the form `key = 'key'`, `key in ('xxx_'), from conjunctions in the WHERE clause.
  * TODO support key like search
  */
std::pair<FieldVectorPtr, bool> getFilterKeys(
    const std::string & primary_key, const DataTypePtr & primary_key_type, const SelectQueryInfo & query_info, const ContextPtr & context);

std::pair<std::shared_ptr<std::vector<FieldVector>>, bool> getFilterKeys(
    const std::vector<String> & primary_key, const std::vector<DataTypePtr> & primary_key_types, const ActionDAGNodes & filter_nodes, const ContextPtr & context);

template <typename K, typename V>
void fillColumns(const K & key, const V & value, size_t key_pos, const Block & header, MutableColumns & columns)
{
    ReadBufferFromString key_buffer(key);
    ReadBufferFromString value_buffer(value);
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & serialization = header.getByPosition(i).type->getDefaultSerialization();
        serialization->deserializeBinary(*columns[i], i == key_pos ? key_buffer : value_buffer, {});
    }
}

template <typename S>
void fillColumns(const S & slice, const std::vector<size_t>& pos, const Block & header, MutableColumns & columns)
{
    ReadBufferFromString buffer(slice);
    for (const auto col : pos) {
        const auto & serialization = header.getByPosition(col).type->getDefaultSerialization();
        serialization->deserializeBinary(*columns[col], buffer, {});
    }
}

std::vector<std::string> serializeKeysToRawString(
    const std::vector<FieldVector> & keys,
    std::vector<size_t> & key_indices,
    const std::vector<DataTypePtr>& key_column_types,
    size_t limit);

std::vector<std::string> serializeKeysToRawString(
    FieldVector::const_iterator & it,
    FieldVector::const_iterator end,
    DataTypePtr key_column_type,
    size_t max_block_size);

std::vector<std::string> serializeKeysToRawString(const ColumnWithTypeAndName & keys);

/// In current implementation key with only column is supported.
size_t getPrimaryKeyPos(const Block & header, const Names & primary_key);

}
