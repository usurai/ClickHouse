#pragma once

#include <Storages/SelectQueryInfo.h>

#include <Interpreters/PreparedSets.h>
#include <Core/Field.h>

#include <IO/ReadBufferFromString.h>

namespace DB
{

using FieldVectorPtr = std::shared_ptr<FieldVector>;
using FieldVectorsPtr = std::shared_ptr<std::vector<FieldVector>>;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

// TODO: comment & move to .cpp
class KeyIterator {
public:
    KeyIterator() = delete;

    KeyIterator(FieldVectorsPtr keys_, size_t begin = 0, size_t keys_to_process = 0)
        : keys{keys_}
    , key_value_indices(keys->size())
    , keys_remaining(keys_to_process)
    {
        std::vector<size_t> size_products(columns(), 1);
        for (int32_t i = static_cast<int32_t>(keys->size()) - 2; i >= 0; --i)
            size_products[i] = size_products[i + 1] * keys->at(i + 1).size();
        const auto total_keys = size_products[0] * keys->at(0).size();
        if (keys_remaining == 0)
            keys_remaining = total_keys - begin;
        assert(begin + keys_to_process < total_keys);
        for (size_t i = 0; i < columns(); ++i)
        {
            key_value_indices[i] = begin / size_products[i];
            begin -= key_value_indices[i] * size_products[i];
        }
    }

    size_t columns() const { return keys->size(); }

    const Field & keyValueAt(size_t column) const
    {
        assert(column < columns());
        assert(!atEnd());
        return keys->at(column)[key_value_indices[column]];
    }

    void advance()
    {
        assert(!atEnd());
        for (size_t i = columns() - 1; ; --i)
        {
            ++key_value_indices[i];
            if (key_value_indices[i] < keys->at(i).size())
                return;
            if (i == 0)
                return;
            key_value_indices[i] = 0;
        }
    }

    bool atEnd() const { return key_value_indices[0] == keys->at(0).size(); }

private:
    FieldVectorsPtr keys;
    std::vector<size_t> key_value_indices;
    size_t keys_remaining;
};

/** Retrieve from the query a condition of the form `key = 'key'`, `key in ('xxx_'), from conjunctions in the WHERE clause.
  * TODO support key like search
  */
std::pair<FieldVectorPtr, bool> getFilterKeys(
    const std::string & primary_key, const DataTypePtr & primary_key_type, const SelectQueryInfo & query_info, const ContextPtr & context);

/** Multi-column primary key version.
 * TODO: Elaborate
 */
std::pair<FieldVectorsPtr, bool> getFilterKeys(
    const Names & primary_key, const DataTypes & primary_key_types, const ActionDAGNodes & filter_nodes, const ContextPtr & context);

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
void fillColumns(const S & slice, const std::vector<size_t> & pos, const Block & header, MutableColumns & columns)
{
    ReadBufferFromString buffer(slice);
    for (const auto col : pos) {
        const auto & serialization = header.getByPosition(col).type->getDefaultSerialization();
        serialization->deserializeBinary(*columns[col], buffer, {});
    }
}

// TODO: comment
std::vector<std::string> serializeKeysToRawString(
    KeyIterator& key_iterator,
    const DataTypes & key_column_types,
    size_t max_block_size);

std::vector<std::string> serializeKeysToRawString(
    FieldVector::const_iterator & it,
    FieldVector::const_iterator end,
    DataTypePtr key_column_type,
    size_t max_block_size);

std::vector<std::string> serializeKeysToRawString(const ColumnWithTypeAndName & keys);

/// In current implementation key with only column is supported.
size_t getPrimaryKeyPos(const Block & header, const Names & primary_key);

}
