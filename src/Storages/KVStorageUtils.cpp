#include <Storages/KVStorageUtils.h>

#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>

#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Functions/IFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
// returns keys may be filter by condition
bool traverseASTFilter(
    const std::string & primary_key, const DataTypePtr & primary_key_type, const ASTPtr & elem, const PreparedSetsPtr & prepared_sets, const ContextPtr & context, FieldVectorPtr & res)
{
    const auto * function = elem->as<ASTFunction>();
    if (!function)
        return false;

    if (function->name == "and")
    {
        // one child has the key filter condition is ok
        for (const auto & child : function->arguments->children)
            if (traverseASTFilter(primary_key, primary_key_type, child, prepared_sets, context, res))
                return true;
        return false;
    }
    else if (function->name == "or")
    {
        // make sure every child has the key filter condition
        for (const auto & child : function->arguments->children)
            if (!traverseASTFilter(primary_key, primary_key_type, child, prepared_sets, context, res))
                return false;
        return true;
    }
    else if (function->name == "equals" || function->name == "in")
    {
        const auto & args = function->arguments->as<ASTExpressionList &>();
        const ASTIdentifier * ident;
        std::shared_ptr<IAST> value;

        if (args.children.size() != 2)
            return false;

        if (function->name == "in")
        {
            if (!prepared_sets)
                return false;

            ident = args.children.at(0)->as<ASTIdentifier>();
            if (!ident)
                return false;

            if (ident->name() != primary_key)
                return false;
            value = args.children.at(1);

            PreparedSets::Hash set_key = value->getTreeHash(/*ignore_aliases=*/ true);
            FutureSetPtr future_set;

            if ((value->as<ASTSubquery>() || value->as<ASTIdentifier>()))
                future_set = prepared_sets->findSubquery(set_key);
            else
                future_set = prepared_sets->findTuple(set_key, {primary_key_type});

            if (!future_set)
                return false;

            future_set->buildOrderedSetInplace(context);

            auto set = future_set->get();
            if (!set)
                return false;

            if (!set->hasExplicitSetElements())
                return false;

            set->checkColumnsNumber(1);
            const auto & set_column = *set->getSetElements()[0];

            if (set_column.getDataType() != primary_key_type->getTypeId())
                return false;

            for (size_t row = 0; row < set_column.size(); ++row)
                res->push_back(set_column[row]);
            return true;
        }
        else
        {
            if ((ident = args.children.at(0)->as<ASTIdentifier>()))
                value = args.children.at(1);
            else if ((ident = args.children.at(1)->as<ASTIdentifier>()))
                value = args.children.at(0);
            else
                return false;

            if (ident->name() != primary_key)
                return false;

            const auto node = evaluateConstantExpressionAsLiteral(value, context);
            /// function->name == "equals"
            if (const auto * literal = node->as<ASTLiteral>())
            {
                auto converted_field = convertFieldToType(literal->value, *primary_key_type);
                if (!converted_field.isNull())
                    res->push_back(converted_field);
                return true;
            }
        }
    }
    return false;
}

// TODO: combine different channel, intersect the same channel
void combine(std::vector<FieldVectorPtr> & lhs, std::vector<FieldVectorPtr> & rhs)
{
    assert(lhs.size() == rhs.size());
    for (size_t i = 0; i < lhs.size(); ++i)
        for (size_t j = 0; j < rhs[i]->size(); ++j)
            lhs[i]->push_back(std::move((*rhs[i])[j]));
}

bool traverseDAGFilter(
    const std::unordered_map<std::string, size_t>& primary_key_pos, const std::vector<DataTypePtr>& primary_key_types, const ActionsDAG::Node * elem, const ContextPtr & context, std::vector<FieldVectorPtr> & res) {
    if (elem->type == ActionsDAG::ActionType::ALIAS)
        return traverseDAGFilter(primary_key_pos, primary_key_types, elem->children.at(0), context, res);

    if (elem->type != ActionsDAG::ActionType::FUNCTION)
        return false;

    auto func_name = elem->function_base->getName();

    if (func_name == "and")
    {
        bool found{false};
        for (const auto * child : elem->children)
        {
            // TODO: consider to get rid of shared_ptr
            std::vector<FieldVectorPtr> partial_res;
            for (size_t i = 0; i < primary_key_pos.size(); ++i)
                partial_res.push_back(std::make_shared<FieldVector>());
            if (!traverseDAGFilter(primary_key_pos, primary_key_types, child, context, partial_res))
                continue;
            found = true;
            combine(res, partial_res);
        }
        return found;
    }
    else if (func_name == "or")
    {
        for (const auto * child : elem->children)
        {
            // TODO: consider to get rid of shared_ptr
            std::vector<FieldVectorPtr> partial_res;
            for (size_t i = 0; i < primary_key_pos.size(); ++i)
                partial_res.push_back(std::make_shared<FieldVector>());

            if (!traverseDAGFilter(primary_key_pos, primary_key_types, child, context, partial_res))
                return false;

            auto count_columns = [](const std::vector<FieldVectorPtr>& vec) {
                size_t columns{0};
                for (const auto & vec_ptr : vec)
                    columns += (vec_ptr->empty() ? 0 : 1);
                return columns;
            };

            if (count_columns(partial_res) < primary_key_pos.size()) {
                return false;
            }

            if (count_columns(res)) {
                // TODO: Support combining multiple OR.
                if (primary_key_pos.size() > 1) {
                    return false;
                }
                for (size_t i = 0; i < partial_res[0]->size(); ++i) {
                    res[0]->push_back(std::move((*partial_res[0])[i]));
                }
            } else {
                res = std::move(partial_res);
            }
        }
        return true;
    }
    else if (func_name == "equals" || func_name == "in")
    {
        if (elem->children.size() != 2)
            return false;

        if (func_name == "in")
        {
            const auto * key = elem->children.at(0);
            while (key->type == ActionsDAG::ActionType::ALIAS)
                key = key->children.at(0);

            if (key->type != ActionsDAG::ActionType::INPUT)
                return false;

            if (!primary_key_pos.contains(key->result_name))
                return false;

            const auto * value = elem->children.at(1);
            if (value->type != ActionsDAG::ActionType::COLUMN)
                return false;

            const IColumn * value_col = value->column.get();
            if (const auto * col_const = typeid_cast<const ColumnConst *>(value_col))
                value_col = &col_const->getDataColumn();

            const auto * col_set = typeid_cast<const ColumnSet *>(value_col);
            if (!col_set)
                return false;

            auto future_set = col_set->getData();
            future_set->buildOrderedSetInplace(context);

            auto set = future_set->get();
            if (!set)
                return false;

            if (!set->hasExplicitSetElements())
                return false;

            set->checkColumnsNumber(1);
            const auto & set_column = *set->getSetElements()[0];

            const auto pos = primary_key_pos.at(key->result_name);
            if (set_column.getDataType() != primary_key_types[pos]->getTypeId())
                return false;

            for (size_t row = 0; row < set_column.size(); ++row)
                res[pos]->push_back(set_column[row]);
            return true;
        }
        else
        {
            const auto * key = elem->children.at(0);
            while (key->type == ActionsDAG::ActionType::ALIAS)
                key = key->children.at(0);

            if (key->type != ActionsDAG::ActionType::INPUT)
                return false;

            if (!primary_key_pos.contains(key->result_name))
                return false;

            const auto * value = elem->children.at(1);
            if (value->type != ActionsDAG::ActionType::COLUMN)
                return false;

            const auto pos = primary_key_pos.at(key->result_name);
            auto converted_field = convertFieldToType((*value->column)[0], *primary_key_types[pos]);
            if (!converted_field.isNull())
                res[pos]->push_back(converted_field);
            return true;
        }
    }
    return false;
}


bool traverseDAGFilter(
    const std::string & primary_key, const DataTypePtr & primary_key_type, const ActionsDAG::Node * elem, const ContextPtr & context, FieldVectorPtr & res)
{
    if (elem->type == ActionsDAG::ActionType::ALIAS)
        return traverseDAGFilter(primary_key, primary_key_type, elem->children.at(0), context, res);

    if (elem->type != ActionsDAG::ActionType::FUNCTION)
        return false;

    auto func_name = elem->function_base->getName();

    if (func_name == "and")
    {
        // one child has the key filter condition is ok
        for (const auto * child : elem->children)
            if (traverseDAGFilter(primary_key, primary_key_type, child, context, res))
                return true;
        return false;
    }
    else if (func_name == "or")
    {
        // make sure every child has the key filter condition
        for (const auto * child : elem->children)
            if (!traverseDAGFilter(primary_key, primary_key_type, child, context, res))
                return false;
        return true;
    }
    else if (func_name == "equals" || func_name == "in")
    {
        if (elem->children.size() != 2)
            return false;

        if (func_name == "in")
        {
            const auto * key = elem->children.at(0);
            while (key->type == ActionsDAG::ActionType::ALIAS)
                key = key->children.at(0);

            if (key->type != ActionsDAG::ActionType::INPUT)
                return false;

            if (key->result_name != primary_key)
                return false;

            const auto * value = elem->children.at(1);
            if (value->type != ActionsDAG::ActionType::COLUMN)
                return false;

            const IColumn * value_col = value->column.get();
            if (const auto * col_const = typeid_cast<const ColumnConst *>(value_col))
                value_col = &col_const->getDataColumn();

            const auto * col_set = typeid_cast<const ColumnSet *>(value_col);
            if (!col_set)
                return false;

            auto future_set = col_set->getData();
            future_set->buildOrderedSetInplace(context);

            auto set = future_set->get();
            if (!set)
                return false;

            if (!set->hasExplicitSetElements())
                return false;

            set->checkColumnsNumber(1);
            const auto & set_column = *set->getSetElements()[0];

            if (set_column.getDataType() != primary_key_type->getTypeId())
                return false;

            for (size_t row = 0; row < set_column.size(); ++row)
                res->push_back(set_column[row]);
            return true;
        }
        else
        {
            const auto * key = elem->children.at(0);
            while (key->type == ActionsDAG::ActionType::ALIAS)
                key = key->children.at(0);

            if (key->type != ActionsDAG::ActionType::INPUT)
                return false;

            if (key->result_name != primary_key)
                return false;

            const auto * value = elem->children.at(1);
            if (value->type != ActionsDAG::ActionType::COLUMN)
                return false;

            auto converted_field = convertFieldToType((*value->column)[0], *primary_key_type);
            if (!converted_field.isNull())
                res->push_back(converted_field);
            return true;
        }
    }
    return false;
}
}

std::pair<std::vector<FieldVectorPtr>, bool> getFilterKeys(
    const std::vector<String> & primary_key, const std::vector<DataTypePtr> & primary_key_types, const ActionDAGNodes & filter_nodes, const ContextPtr & context)
{
    if (filter_nodes.nodes.empty())
        return {{}, true};

    auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes);
    const auto * predicate = filter_actions_dag->getOutputs().at(0);

    std::unordered_map<String, size_t> primary_key_pos;
    for (size_t i = 0; i < primary_key.size(); ++i) {
        primary_key_pos[primary_key[i]] = i;
    }

    // TODO: set is better?
    std::vector<FieldVectorPtr> res;
    for (size_t i = 0; i < primary_key.size(); ++i)
        res.push_back(std::make_shared<FieldVector>());
    auto matched_keys = traverseDAGFilter(primary_key_pos, primary_key_types, predicate, context, res);

    std::cout << "keys size:\n";
    for (auto & vec : res) {
        std::cout << vec->size() << ':';
        for (const auto & f : *vec)
            std::cout << toString(f) << ' ';
        std::cout << '\n';
    }
    std::cout << '\n';
    std::cout << "all_scan:" << !matched_keys << '\n';

    // TODO: if not all the columns are filled, return no matched
    return std::make_pair(res, !matched_keys);
}

std::pair<FieldVectorPtr, bool> getFilterKeys(
    const String & primary_key, const DataTypePtr & primary_key_type, const SelectQueryInfo & query_info, const ContextPtr & context)
{
    const auto & select = query_info.query->as<ASTSelectQuery &>();
    if (!select.where())
        return {{}, true};

    FieldVectorPtr res = std::make_shared<FieldVector>();
    auto matched_keys = traverseASTFilter(primary_key, primary_key_type, select.where(), query_info.prepared_sets, context, res);
    return std::make_pair(res, !matched_keys);
}

std::vector<std::string> serializeKeysToRawString(
    FieldVector::const_iterator & it,
    FieldVector::const_iterator end,
    DataTypePtr key_column_type,
    size_t max_block_size)
{
    size_t num_keys = end - it;

    std::vector<std::string> result;
    result.reserve(num_keys);

    size_t rows_processed = 0;
    while (it < end && (max_block_size == 0 || rows_processed < max_block_size))
    {
        std::string & serialized_key = result.emplace_back();
        WriteBufferFromString wb(serialized_key);
        key_column_type->getDefaultSerialization()->serializeBinary(*it, wb, {});
        wb.finalize();

        ++it;
        ++rows_processed;
    }
    return result;
}

std::vector<std::string> serializeKeysToRawString(const ColumnWithTypeAndName & keys)
{
    if (!keys.column)
        return {};

    size_t num_keys = keys.column->size();
    std::vector<std::string> result;
    result.reserve(num_keys);

    for (size_t i = 0; i < num_keys; ++i)
    {
        std::string & serialized_key = result.emplace_back();
        WriteBufferFromString wb(serialized_key);
        Field field;
        keys.column->get(i, field);
        /// TODO(@vdimir): use serializeBinaryBulk
        keys.type->getDefaultSerialization()->serializeBinary(field, wb, {});
        wb.finalize();
    }
    return result;
}

/// In current implementation rocks db/redis can have key with only one column.
size_t getPrimaryKeyPos(const Block & header, const Names & primary_key)
{
    if (primary_key.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Only one primary key is supported");
    return header.getPositionByName(primary_key[0]);
}

}
