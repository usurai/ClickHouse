#include <iomanip>

#include <DB/Core/Field.h>

#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/OneBlockInputStream.h>

#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>

#include <DB/Interpreters/Set.h>


namespace DB
{

Set::Type Set::chooseMethod(Columns & key_columns, bool & keys_fit_128_bits, Sizes & key_sizes)
{
	size_t keys_size = key_columns.size();

	keys_fit_128_bits = true;
	size_t keys_bytes = 0;
	key_sizes.resize(keys_size);
	for (size_t j = 0; j < keys_size; ++j)
	{
		if (!key_columns[j]->isNumeric())
		{
			keys_fit_128_bits = false;
			break;
		}
		key_sizes[j] = key_columns[j]->sizeOfField();
		keys_bytes += key_sizes[j];
	}
	if (keys_bytes > 16)
		keys_fit_128_bits = false;

	/// Если есть один ключ, который помещается в 64 бита, и это не число с плавающей запятой
	if (keys_size == 1 && key_columns[0]->isNumeric()
		&& !dynamic_cast<ColumnFloat32 *>(&*key_columns[0]) && !dynamic_cast<ColumnFloat64 *>(&*key_columns[0])
		&& !dynamic_cast<ColumnConstFloat32 *>(&*key_columns[0]) && !dynamic_cast<ColumnConstFloat64 *>(&*key_columns[0]))
		return KEY_64;

	/// Если есть один строковый ключ, то используем хэш-таблицу с ним
	if (keys_size == 1
		&& (dynamic_cast<ColumnString *>(&*key_columns[0]) || dynamic_cast<ColumnFixedString *>(&*key_columns[0])
			|| dynamic_cast<ColumnConstString *>(&*key_columns[0])))
		return KEY_STRING;

	/// Если много ключей - будем строить множество хэшей от них
	return HASHED;
}


void Set::create(BlockInputStreamPtr stream)
{
	LOG_TRACE(log, "Creating set");
	Stopwatch watch;
	size_t entries = 0;
	
	/// Читаем все данные
	while (Block block = stream->read())
	{
		size_t keys_size = block.columns();
		Row key(keys_size);
		Columns key_columns(keys_size);
		data_types.resize(keys_size);
		
		/// Запоминаем столбцы, с которыми будем работать
		for (size_t i = 0; i < keys_size; ++i)
		{
			key_columns[i] = block.getByPosition(i).column;
			data_types[i] = block.getByPosition(i).type;
		}

		size_t rows = block.rows();

		/// Какую структуру данных для множества использовать?
		bool keys_fit_128_bits = false;
		Sizes key_sizes;
		type = chooseMethod(key_columns, keys_fit_128_bits, key_sizes);

		if (type == KEY_64)
		{
			SetUInt64 & res = key64;
			const FieldVisitorToUInt64 visitor;
			IColumn & column = *key_columns[0];

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				Field field = column[i];
				UInt64 key = boost::apply_visitor(visitor, field);
				res.insert(key);
			}

			entries = res.size();
		}
		else if (type == KEY_STRING)
		{
			SetString & res = key_string;
			IColumn & column = *key_columns[0];

			if (const ColumnString * column_string = dynamic_cast<const ColumnString *>(&column))
			{
				const ColumnString::Offsets_t & offsets = column_string->getOffsets();
				const ColumnUInt8::Container_t & data = dynamic_cast<const ColumnUInt8 &>(column_string->getData()).getData();

				/// Для всех строчек
				for (size_t i = 0; i < rows; ++i)
				{
					/// Строим ключ
					StringRef ref(&data[i == 0 ? 0 : offsets[i - 1]], (i == 0 ? offsets[i] : (offsets[i] - offsets[i - 1])) - 1);

					SetString::iterator it;
					bool inserted;
					res.emplace(ref, it, inserted);

					if (inserted)
						it->data = string_pool.insert(ref.data, ref.size);
				}
			}
			else if (const ColumnFixedString * column_string = dynamic_cast<const ColumnFixedString *>(&column))
			{
				size_t n = column_string->getN();
				const ColumnUInt8::Container_t & data = dynamic_cast<const ColumnUInt8 &>(column_string->getData()).getData();

				/// Для всех строчек
				for (size_t i = 0; i < rows; ++i)
				{
					/// Строим ключ
					StringRef ref(&data[i * n], n);

					SetString::iterator it;
					bool inserted;
					res.emplace(ref, it, inserted);

					if (inserted)
						it->data = string_pool.insert(ref.data, ref.size);
				}
			}
			else
				throw Exception("Illegal type of column when creating set with string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);

			entries = res.size();
		}
		else if (type == HASHED)
		{
			SetHashed & res = hashed;
			const FieldVisitorToUInt64 to_uint64_visitor;

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
				res.insert(pack128(i, keys_fit_128_bits, keys_size, key, key_columns, key_sizes));

			entries = res.size();
		}
		else if (type == GENERIC)
		{
			/// Общий способ
			SetGeneric & res = generic;

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				for (size_t j = 0; j < keys_size; ++j)
					key[j] = (*key_columns[j])[i];

				res.insert(key);
			}

			entries = res.size();
		}
		else
			throw Exception("Unknown set variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
	}

	logProfileInfo(watch, *stream, entries);
}


void Set::logProfileInfo(Stopwatch & watch, IBlockInputStream & in, size_t entries)
{
	/// Выведем информацию о том, сколько считано строк и байт.
	size_t rows = 0;
	size_t bytes = 0;

	in.getLeafRowsBytes(rows, bytes);

	size_t head_rows = 0;
	if (IProfilingBlockInputStream * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&in))
		head_rows = profiling_in->getInfo().rows;

	if (rows != 0)
	{
		LOG_DEBUG(log, std::fixed << std::setprecision(3)
			<< "Created set with " << entries << " entries from " << head_rows << " rows."
			<< " Read " << rows << " rows, " << bytes / 1048576.0 << " MiB in " << watch.elapsedSeconds() << " sec., "
			<< static_cast<size_t>(rows / watch.elapsedSeconds()) << " rows/sec., " << bytes / 1048576.0 / watch.elapsedSeconds() << " MiB/sec.");
	}
}


void Set::create(DataTypes & types, ASTPtr node)
{
	data_types = types;

	/// Засунем множество в блок.
	Block block;
	for (size_t i = 0, size = data_types.size(); i < size; ++i)
	{
		ColumnWithNameAndType col;
		col.type = data_types[i];
		col.column = data_types[i]->createColumn();
		col.name = "_" + Poco::NumberFormatter::format(i);

		block.insert(col);
	}

	ASTExpressionList & list = dynamic_cast<ASTExpressionList &>(*node);
	for (ASTs::iterator it = list.children.begin(); it != list.children.end(); ++it)
	{
		if (data_types.size() == 1)
		{
			if (ASTLiteral * lit = dynamic_cast<ASTLiteral *>(&**it))
				block.getByPosition(0).column->insert(lit->value);
			else
				throw Exception("Incorrect element of set. Must be literal.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
		}
		else if (ASTFunction * func = dynamic_cast<ASTFunction *>(&**it))
		{
			if (func->name != "tuple")
				throw Exception("Incorrect element of set. Must be tuple.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);

			size_t tuple_size = func->arguments->children.size();
			if (tuple_size != data_types.size())
				throw Exception("Incorrect size of tuple in set.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
			
			for (size_t j = 0; j < tuple_size; ++j)
			{
				if (ASTLiteral * lit = dynamic_cast<ASTLiteral *>(&*func->arguments->children[j]))
					block.getByPosition(j).column->insert(lit->value);
				else
					throw Exception("Incorrect element of tuple in set. Must be literal.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
			}
		}
		else
			throw Exception("Incorrect element of set", ErrorCodes::INCORRECT_ELEMENT_OF_SET);

		/// NOTE: Потом можно реализовать возможность задавать константные выражения в множествах.
	}

	create(new OneBlockInputStream(block));
}


void Set::execute(Block & block, const ColumnNumbers & arguments, size_t result, bool negative) const
{
	LOG_TRACE(log, "Checking set membership for block.");

	ColumnUInt8 * c_res = new ColumnUInt8;
	block.getByPosition(result).column = c_res;
	ColumnUInt8::Container_t & vec_res = c_res->getData();
	vec_res.resize(block.getByPosition(arguments[0]).column->size());

	size_t keys_size = arguments.size();
	Row key(keys_size);
	Columns key_columns(keys_size);

	/// Запоминаем столбцы, с которыми будем работать. Также проверим, что типы данных правильные.
	for (size_t i = 0; i < keys_size; ++i)
	{
		key_columns[i] = block.getByPosition(arguments[i]).column;

		if (data_types[i]->getName() != block.getByPosition(arguments[i]).type->getName())
			throw Exception("Types in section IN doesn't match.", ErrorCodes::TYPE_MISMATCH);
	}

	size_t rows = block.rows();

	/// Какую структуру данных для множества использовать?
	bool keys_fit_128_bits = false;
	Sizes key_sizes;
	if (type != chooseMethod(key_columns, keys_fit_128_bits, key_sizes))
		throw Exception("Incompatible columns in IN section", ErrorCodes::INCOMPATIBLE_COLUMNS);

	if (type == KEY_64)
	{
		const SetUInt64 & set = key64;
		const FieldVisitorToUInt64 visitor;
		IColumn & column = *key_columns[0];

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			/// Строим ключ
			Field field = column[i];
			UInt64 key = boost::apply_visitor(visitor, field);
			vec_res[i] = negative ^ (set.end() != set.find(key));
		}
	}
	else if (type == KEY_STRING)
	{
		const SetString & set = key_string;
		IColumn & column = *key_columns[0];

		if (const ColumnString * column_string = dynamic_cast<const ColumnString *>(&column))
		{
			const ColumnString::Offsets_t & offsets = column_string->getOffsets();
			const ColumnUInt8::Container_t & data = dynamic_cast<const ColumnUInt8 &>(column_string->getData()).getData();

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				StringRef ref(&data[i == 0 ? 0 : offsets[i - 1]], (i == 0 ? offsets[i] : (offsets[i] - offsets[i - 1])) - 1);
				vec_res[i] = negative ^ (set.end() != set.find(ref));
			}
		}
		else if (const ColumnFixedString * column_string = dynamic_cast<const ColumnFixedString *>(&column))
		{
			size_t n = column_string->getN();
			const ColumnUInt8::Container_t & data = dynamic_cast<const ColumnUInt8 &>(column_string->getData()).getData();

			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
			{
				/// Строим ключ
				StringRef ref(&data[i * n], n);
				vec_res[i] = negative ^ (set.end() != set.find(ref));
			}
		}
		else if (const ColumnConstString * column_string = dynamic_cast<const ColumnConstString *>(&column))
		{
			bool res = negative ^ (set.end() != set.find(StringRef(column_string->getData())));
			
			/// Для всех строчек
			for (size_t i = 0; i < rows; ++i)
				vec_res[i] = res;
		}
		else
			throw Exception("Illegal type of column when creating set with string key: " + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
	}
	else if (type == HASHED)
	{
		const SetHashed & set = hashed;
		const FieldVisitorToUInt64 to_uint64_visitor;

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
			vec_res[i] = negative ^ (set.end() != set.find(pack128(i, keys_fit_128_bits, keys_size, key, key_columns, key_sizes)));
	}
	else if (type == GENERIC)
	{
		/// Общий способ
		const SetGeneric & set = generic;

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			/// Строим ключ
			for (size_t j = 0; j < keys_size; ++j)
				key[j] = (*key_columns[j])[i];

			vec_res[i] = negative ^ (set.end() != set.find(key));
		}
	}
	else
		throw Exception("Unknown set variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);

	LOG_TRACE(log, "Checked set membership for block.");
}

}
