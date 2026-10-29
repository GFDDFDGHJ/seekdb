/**
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_file_parquet_reader.h"

#include <cstring>

namespace oceanbase
{
namespace sql
{

namespace
{
const char PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

/* the thrift compact protocol types */
const int32_t THRIFT_STOP = 0;
const int32_t THRIFT_BOOLEAN_TRUE = 1;
const int32_t THRIFT_BOOLEAN_FALSE = 2;
const int32_t THRIFT_BYTE = 3;
const int32_t THRIFT_I16 = 4;
const int32_t THRIFT_I32 = 5;
const int32_t THRIFT_I64 = 6;
const int32_t THRIFT_DOUBLE = 7;
const int32_t THRIFT_BINARY = 8;
const int32_t THRIFT_LIST = 9;
const int32_t THRIFT_SET = 10;
const int32_t THRIFT_MAP = 11;
const int32_t THRIFT_STRUCT = 12;

/* the parquet page types */
const int32_t PAGE_DATA = 0;
const int32_t PAGE_INDEX = 1;
const int32_t PAGE_DICTIONARY = 2;
const int32_t PAGE_DATA_V2 = 3;

/**
 * @brief A reader of the thrift compact protocol.
 */
class ObThriftCompactReader
{
public:
  ObThriftCompactReader() : data_(NULL), length_(0), pos_(0), depth_(0)
  {
    for (int i = 0; i < 32; ++i) {
      last_field_id_[i] = 0;
    }
  }

  int init(const char *data, int64_t length)
  {
    data_ = data;
    length_ = length;
    pos_ = 0;
    depth_ = 0;
    for (int i = 0; i < 32; ++i) {
      last_field_id_[i] = 0;
    }
    return OB_SUCCESS;
  }

  int64_t get_pos() const { return pos_; }
  int64_t get_remaining() const { return length_ - pos_; }

  int read_byte(uint8_t &value)
  {
    int ret = OB_SUCCESS;
    if (pos_ >= length_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      value = static_cast<uint8_t>(data_[pos_++]);
    }
    return ret;
  }

  int read_varint(uint64_t &value)
  {
    int ret = OB_SUCCESS;
    uint64_t result = 0;
    int shift = 0;
    while (true) {
      uint8_t byte = 0;
      if (OB_FAIL(read_byte(byte))) {
        return ret;
      }
      result |= static_cast<uint64_t>(byte & 0x7F) << shift;
      if (0 == (byte & 0x80)) {
        break;
      }
      shift += 7;
      if (shift > 63) {
        return OB_ERR_UNEXPECTED;
      }
    }
    value = result;
    return ret;
  }

  int read_zigzag(int64_t &value)
  {
    int ret = OB_SUCCESS;
    uint64_t raw = 0;
    if (OB_FAIL(read_varint(raw))) {
      return ret;
    }
    value = static_cast<int64_t>((raw >> 1) ^ (~(raw & 1) + 1));
    return ret;
  }

  int read_i32(int32_t &value)
  {
    int ret = OB_SUCCESS;
    int64_t tmp = 0;
    if (OB_FAIL(read_zigzag(tmp))) {
      return ret;
    }
    value = static_cast<int32_t>(tmp);
    return ret;
  }

  int read_i64(int64_t &value)
  {
    int ret = OB_SUCCESS;
    int64_t tmp = 0;
    if (OB_FAIL(read_zigzag(tmp))) {
      return ret;
    }
    value = tmp;
    return ret;
  }

  int read_binary(std::string &value)
  {
    int ret = OB_SUCCESS;
    uint64_t length = 0;
    if (OB_FAIL(read_varint(length))) {
      return ret;
    }
    if (pos_ + static_cast<int64_t>(length) > length_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      value.assign(data_ + pos_, static_cast<size_t>(length));
      pos_ += static_cast<int64_t>(length);
    }
    return ret;
  }

  int read_list_header(int32_t &size, int8_t &elem_type)
  {
    int ret = OB_SUCCESS;
    uint8_t byte = 0;
    if (OB_FAIL(read_byte(byte))) {
      return ret;
    }
    elem_type = static_cast<int8_t>(byte & 0x0F);
    int32_t count = static_cast<int32_t>((byte & 0xF0) >> 4);
    if (15 == count) {
      uint64_t tmp = 0;
      if (OB_FAIL(read_varint(tmp))) {
        return ret;
      }
      count = static_cast<int32_t>(tmp);
    }
    size = count;
    return ret;
  }

  int push_struct()
  {
    int ret = OB_SUCCESS;
    if (depth_ >= 32) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      last_field_id_[depth_] = 0;
      ++depth_;
    }
    return ret;
  }

  int pop_struct()
  {
    if (depth_ > 0) {
      --depth_;
    }
    return OB_SUCCESS;
  }

  /**
   * @brief Read the header of the next field of the current struct.
   */
  int read_field_header(int32_t &type, int32_t &field_id, bool &is_stop)
  {
    int ret = OB_SUCCESS;
    uint8_t byte = 0;
    if (OB_FAIL(read_byte(byte))) {
      return ret;
    }
    if (THRIFT_STOP == byte) {
      is_stop = true;
      type = THRIFT_STOP;
      field_id = 0;
      return ret;
    }
    is_stop = false;
    type = byte & 0x0F;
    const int32_t delta = (byte & 0xF0) >> 4;
    const int32_t slot = (depth_ > 0) ? depth_ - 1 : 0;
    if (0 == delta) {
      int64_t tmp = 0;
      if (OB_FAIL(read_zigzag(tmp))) {
        return ret;
      }
      field_id = static_cast<int32_t>(tmp);
    } else {
      field_id = last_field_id_[slot] + delta;
    }
    last_field_id_[slot] = field_id;
    return ret;
  }

  /**
   * @brief Skip a value of the given type.
   */
  int skip(int32_t type)
  {
    int ret = OB_SUCCESS;
    switch (type) {
    case THRIFT_BOOLEAN_TRUE:
    case THRIFT_BOOLEAN_FALSE:
      break;
    case THRIFT_BYTE: {
      uint8_t byte = 0;
      ret = read_byte(byte);
      break;
    }
    case THRIFT_I16:
    case THRIFT_I32:
    case THRIFT_I64: {
      int64_t tmp = 0;
      ret = read_zigzag(tmp);
      break;
    }
    case THRIFT_DOUBLE: {
      if (pos_ + 8 > length_) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        pos_ += 8;
      }
      break;
    }
    case THRIFT_BINARY: {
      std::string tmp;
      ret = read_binary(tmp);
      break;
    }
    case THRIFT_LIST:
    case THRIFT_SET: {
      int32_t size = 0;
      int8_t elem_type = 0;
      if (OB_FAIL(read_list_header(size, elem_type))) {
      } else {
        for (int32_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          ret = skip(elem_type);
        }
      }
      break;
    }
    case THRIFT_MAP: {
      uint64_t size = 0;
      if (OB_FAIL(read_varint(size))) {
      } else if (size > 0) {
        uint8_t types = 0;
        if (OB_FAIL(read_byte(types))) {
        } else {
          const int8_t key_type = static_cast<int8_t>((types & 0xF0) >> 4);
          const int8_t value_type = static_cast<int8_t>(types & 0x0F);
          for (uint64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
            if (OB_FAIL(skip(key_type))) {
            } else {
              ret = skip(value_type);
            }
          }
        }
      }
      break;
    }
    case THRIFT_STRUCT: {
      if (OB_FAIL(push_struct())) {
      } else {
        while (true) {
          int32_t field_type = 0;
          int32_t field_id = 0;
          bool is_stop = false;
          if (OB_FAIL(read_field_header(field_type, field_id, is_stop))) {
            break;
          }
          if (is_stop) {
            break;
          }
          if (OB_FAIL(skip(field_type))) {
            break;
          }
        }
        pop_struct();
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    return ret;
  }

private:
  const char *data_;
  int64_t length_;
  int64_t pos_;
  int32_t last_field_id_[32];
  int32_t depth_;
};

inline int32_t bit_width_of(int32_t max_value)
{
  int32_t width = 0;
  while (max_value > 0) {
    ++width;
    max_value >>= 1;
  }
  return width;
}
} // namespace

int ObParquetMemorySource::read(int64_t offset, int64_t length, char *buf)
{
  int ret = OB_SUCCESS;
  if (offset < 0 || length < 0 || offset + length > length_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (length > 0) {
    memcpy(buf, data_ + offset, static_cast<size_t>(length));
  }
  return ret;
}

ObFileParquetReader::ObFileParquetReader() : source_(NULL), num_rows_(0) {}

ObFileParquetReader::~ObFileParquetReader() {}

int ObFileParquetReader::init(ObParquetFileSource *source)
{
  int ret = OB_SUCCESS;
  if (NULL == source) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    source_ = source;
    if (OB_FAIL(read_footer())) {
      source_ = NULL;
    }
  }
  return ret;
}

int ObFileParquetReader::read_footer()
{
  int ret = OB_SUCCESS;
  const int64_t file_size = (NULL == source_) ? 0 : source_->get_size();
  if (file_size < 12) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    char magic[4] = {'\0', '\0', '\0', '\0'};
    if (OB_FAIL(source_->read(0, 4, magic))) {
    } else if (0 != memcmp(magic, PARQUET_MAGIC, 4)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(source_->read(file_size - 4, 4, magic))) {
    } else if (0 != memcmp(magic, PARQUET_MAGIC, 4)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      char length_buf[4] = {'\0', '\0', '\0', '\0'};
      if (OB_FAIL(source_->read(file_size - 8, 4, length_buf))) {
      } else {
        uint32_t footer_length = 0;
        for (int i = 0; i < 4; ++i) {
          footer_length |= static_cast<uint32_t>(static_cast<uint8_t>(length_buf[i])) << (8 * i);
        }
        const int64_t footer_offset = file_size - 8 - static_cast<int64_t>(footer_length);
        if (footer_offset < 4 || static_cast<int64_t>(footer_length) <= 0) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          std::string footer;
          footer.resize(footer_length);
          if (OB_FAIL(source_->read(footer_offset, footer_length, &footer[0]))) {
          } else if (OB_FAIL(parse_file_metadata(footer.data(), footer_length))) {
          }
        }
      }
    }
  }
  return ret;
}

static int parse_schema_element(ObThriftCompactReader &reader,
                                ObParquetColumnMeta &column,
                                bool &is_root,
                                int32_t &num_children);
static int parse_row_group(ObThriftCompactReader &reader, ObParquetRowGroupMeta &row_group);
static int parse_column_chunk(ObThriftCompactReader &reader, ObParquetColumnChunkMeta &chunk);
static int parse_column_metadata(ObThriftCompactReader &reader, ObParquetColumnChunkMeta &chunk);

int ObFileParquetReader::parse_file_metadata(const char *data, int64_t length)
{
  int ret = OB_SUCCESS;
  ObThriftCompactReader reader;
  if (OB_FAIL(reader.init(data, length))) {
    return ret;
  }
  if (OB_FAIL(reader.push_struct())) {
    return ret;
  }
  bool schema_found = false;
  while (true) {
    int32_t field_type = 0;
    int32_t field_id = 0;
    bool is_stop = false;
    if (OB_FAIL(reader.read_field_header(field_type, field_id, is_stop))) {
      break;
    }
    if (is_stop) {
      break;
    }
    if (3 == field_id && THRIFT_I64 == field_type) {
      if (OB_FAIL(reader.read_i64(num_rows_))) {
        break;
      }
    } else if (2 == field_id && THRIFT_LIST == field_type) {
      int32_t size = 0;
      int8_t elem_type = 0;
      if (OB_FAIL(reader.read_list_header(size, elem_type))) {
        break;
      }
      schema_found = true;
      for (int32_t i = 0; OB_SUCC(ret) && i < size; ++i) {
        ObParquetColumnMeta column;
        bool is_root = false;
        int32_t num_children = -1;
        if (OB_FAIL(parse_schema_element(reader, column, is_root, num_children))) {
          break;
        }
        if (0 != i) {
          columns_.push_back(column);
        }
      }
    } else if (4 == field_id && THRIFT_LIST == field_type) {
      int32_t size = 0;
      int8_t elem_type = 0;
      if (OB_FAIL(reader.read_list_header(size, elem_type))) {
        break;
      }
      for (int32_t i = 0; OB_SUCC(ret) && i < size; ++i) {
        ObParquetRowGroupMeta row_group;
        if (OB_FAIL(parse_row_group(reader, row_group))) {
          break;
        }
        row_groups_.push_back(row_group);
      }
    } else if (OB_FAIL(reader.skip(field_type))) {
      break;
    }
  }
  reader.pop_struct();
  if (OB_SUCC(ret) && !schema_found) {
    ret = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCC(ret)) {
    // the maximum definition level of a flat schema is one when the column is optional
    for (size_t i = 0; i < columns_.size(); ++i) {
      columns_[i].max_def_level_ =
          (ObParquetRepetition::OPTIONAL == columns_[i].repetition_type_) ? 1 : 0;
      columns_[i].max_rep_level_ =
          (ObParquetRepetition::REPEATED == columns_[i].repetition_type_) ? 1 : 0;
    }
  }
  return ret;
}

static int parse_schema_element(ObThriftCompactReader &reader,
                                              ObParquetColumnMeta &column,
                                              bool &is_root,
                                              int32_t &num_children)
{
  int ret = OB_SUCCESS;
  is_root = false;
  num_children = -1;
  if (OB_FAIL(reader.push_struct())) {
    return ret;
  }
  while (true) {
    int32_t field_type = 0;
    int32_t field_id = 0;
    bool is_stop = false;
    if (OB_FAIL(reader.read_field_header(field_type, field_id, is_stop))) {
      break;
    }
    if (is_stop) {
      break;
    }
    if (1 == field_id && THRIFT_I32 == field_type) {
      int32_t value = 0;
      if (OB_FAIL(reader.read_i32(value))) {
        break;
      }
      column.type_ = static_cast<ObParquetType>(value);
    } else if (2 == field_id && THRIFT_I32 == field_type) {
      int32_t value = 0;
      if (OB_FAIL(reader.read_i32(value))) {
        break;
      }
      column.type_length_ = value;
    } else if (3 == field_id && THRIFT_I32 == field_type) {
      int32_t value = 0;
      if (OB_FAIL(reader.read_i32(value))) {
        break;
      }
      column.repetition_type_ = static_cast<ObParquetRepetition>(value);
    } else if (4 == field_id && THRIFT_BINARY == field_type) {
      std::string value;
      if (OB_FAIL(reader.read_binary(value))) {
        break;
      }
      column.name_ = value;
    } else if (5 == field_id && THRIFT_I32 == field_type) {
      int32_t value = 0;
      if (OB_FAIL(reader.read_i32(value))) {
        break;
      }
      num_children = value;
    } else if (6 == field_id && THRIFT_I32 == field_type) {
      int32_t value = 0;
      if (OB_FAIL(reader.read_i32(value))) {
        break;
      }
      column.converted_type_ = value;
    } else if (OB_FAIL(reader.skip(field_type))) {
      break;
    }
  }
  reader.pop_struct();
  return ret;
}

static int parse_row_group(ObThriftCompactReader &reader, ObParquetRowGroupMeta &row_group)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader.push_struct())) {
    return ret;
  }
  while (true) {
    int32_t field_type = 0;
    int32_t field_id = 0;
    bool is_stop = false;
    if (OB_FAIL(reader.read_field_header(field_type, field_id, is_stop))) {
      break;
    }
    if (is_stop) {
      break;
    }
    if (1 == field_id && THRIFT_LIST == field_type) {
      int32_t size = 0;
      int8_t elem_type = 0;
      if (OB_FAIL(reader.read_list_header(size, elem_type))) {
        break;
      }
      for (int32_t i = 0; OB_SUCC(ret) && i < size; ++i) {
        ObParquetColumnChunkMeta chunk;
        if (OB_FAIL(parse_column_chunk(reader, chunk))) {
          break;
        }
        row_group.chunks_.push_back(chunk);
      }
    } else if (2 == field_id && THRIFT_I64 == field_type) {
      if (OB_FAIL(reader.read_i64(row_group.total_byte_size_))) {
        break;
      }
    } else if (3 == field_id && THRIFT_I64 == field_type) {
      if (OB_FAIL(reader.read_i64(row_group.num_rows_))) {
        break;
      }
    } else if (OB_FAIL(reader.skip(field_type))) {
      break;
    }
  }
  reader.pop_struct();
  return ret;
}

static int parse_column_chunk(ObThriftCompactReader &reader, ObParquetColumnChunkMeta &chunk)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader.push_struct())) {
    return ret;
  }
  while (true) {
    int32_t field_type = 0;
    int32_t field_id = 0;
    bool is_stop = false;
    if (OB_FAIL(reader.read_field_header(field_type, field_id, is_stop))) {
      break;
    }
    if (is_stop) {
      break;
    }
    if (2 == field_id && THRIFT_I64 == field_type) {
      if (OB_FAIL(reader.read_i64(chunk.file_offset_))) {
        break;
      }
    } else if (3 == field_id && THRIFT_STRUCT == field_type) {
      if (OB_FAIL(parse_column_metadata(reader, chunk))) {
        break;
      }
    } else if (OB_FAIL(reader.skip(field_type))) {
      break;
    }
  }
  reader.pop_struct();
  return ret;
}

static int parse_column_metadata(ObThriftCompactReader &reader, ObParquetColumnChunkMeta &chunk)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader.push_struct())) {
    return ret;
  }
  while (true) {
    int32_t field_type = 0;
    int32_t field_id = 0;
    bool is_stop = false;
    if (OB_FAIL(reader.read_field_header(field_type, field_id, is_stop))) {
      break;
    }
    if (is_stop) {
      break;
    }
    if (1 == field_id && THRIFT_I32 == field_type) {
      int32_t value = 0;
      if (OB_FAIL(reader.read_i32(value))) {
        break;
      }
      chunk.type_ = static_cast<ObParquetType>(value);
    } else if (4 == field_id && THRIFT_I32 == field_type) {
      int32_t value = 0;
      if (OB_FAIL(reader.read_i32(value))) {
        break;
      }
      chunk.codec_ = static_cast<ObParquetCodec>(value);
    } else if (5 == field_id && THRIFT_I64 == field_type) {
      if (OB_FAIL(reader.read_i64(chunk.num_values_))) {
        break;
      }
    } else if (6 == field_id && THRIFT_I64 == field_type) {
      if (OB_FAIL(reader.read_i64(chunk.total_uncompressed_size_))) {
        break;
      }
    } else if (7 == field_id && THRIFT_I64 == field_type) {
      if (OB_FAIL(reader.read_i64(chunk.total_compressed_size_))) {
        break;
      }
    } else if (9 == field_id && THRIFT_I64 == field_type) {
      if (OB_FAIL(reader.read_i64(chunk.data_page_offset_))) {
        break;
      }
    } else if (11 == field_id && THRIFT_I64 == field_type) {
      if (OB_FAIL(reader.read_i64(chunk.dictionary_page_offset_))) {
        break;
      }
    } else if (OB_FAIL(reader.skip(field_type))) {
      break;
    }
  }
  reader.pop_struct();
  return ret;
}

/* ------------------------------------------------------------------------- */
/* data pages                                                                */
/* ------------------------------------------------------------------------- */

int ObFileParquetReader::read_column(int64_t row_group_index,
                                     int64_t column_index,
                                     std::vector<ObParquetValue> &values)
{
  int ret = OB_SUCCESS;
  if (row_group_index < 0 || row_group_index >= static_cast<int64_t>(row_groups_.size())
      || column_index < 0 || column_index >= static_cast<int64_t>(columns_.size())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const ObParquetRowGroupMeta &row_group = row_groups_[row_group_index];
    const ObParquetColumnMeta &column = columns_[column_index];
    if (column_index >= static_cast<int64_t>(row_group.chunks_.size())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const ObParquetColumnChunkMeta &chunk = row_group.chunks_[column_index];
      int64_t offset = chunk.data_page_offset_;
      if (chunk.dictionary_page_offset_ > 0 && chunk.dictionary_page_offset_ < offset) {
        offset = chunk.dictionary_page_offset_;
      }
      const int64_t chunk_end = offset + chunk.total_compressed_size_;
      std::string dictionary;
      int64_t num_values = 0;
      while (OB_SUCC(ret) && num_values < chunk.num_values_ && offset < chunk_end) {
        // read the page header
        char header_buf[4096];
        const int64_t header_read = std::min<int64_t>(chunk_end - offset, sizeof(header_buf));
        if (OB_FAIL(source_->read(offset, header_read, header_buf))) {
          break;
        }
        ObThriftCompactReader reader;
        if (OB_FAIL(reader.init(header_buf, header_read))) {
          break;
        }
        if (OB_FAIL(reader.push_struct())) {
          break;
        }
        int32_t page_type = -1;
        int64_t uncompressed_size = 0;
        int64_t compressed_size = 0;
        int64_t num_page_values = 0;
        int32_t encoding = 0;
        int32_t def_level_encoding = 0;
        int32_t rep_level_encoding = 0;
        int64_t def_levels_byte_length = 0;
        int64_t rep_levels_byte_length = 0;
        bool is_compressed = false;
        while (true) {
          int32_t field_type = 0;
          int32_t field_id = 0;
          bool is_stop = false;
          if (OB_FAIL(reader.read_field_header(field_type, field_id, is_stop))) {
            break;
          }
          if (is_stop) {
            break;
          }
          if (1 == field_id && THRIFT_I32 == field_type) {
            int32_t value = 0;
            if (OB_FAIL(reader.read_i32(value))) {
              break;
            }
            page_type = value;
          } else if (2 == field_id && THRIFT_I32 == field_type) {
            int32_t value = 0;
            if (OB_FAIL(reader.read_i32(value))) {
              break;
            }
            uncompressed_size = value;
          } else if (3 == field_id && THRIFT_I32 == field_type) {
            int32_t value = 0;
            if (OB_FAIL(reader.read_i32(value))) {
              break;
            }
            compressed_size = value;
          } else if (5 == field_id && THRIFT_STRUCT == field_type) {
            // the data page header of the version one pages
            if (OB_FAIL(reader.push_struct())) {
              break;
            }
            while (true) {
              int32_t sub_type = 0;
              int32_t sub_id = 0;
              bool sub_stop = false;
              if (OB_FAIL(reader.read_field_header(sub_type, sub_id, sub_stop))) {
                break;
              }
              if (sub_stop) {
                break;
              }
              if (1 == sub_id && THRIFT_I32 == sub_type) {
                int32_t value = 0;
                if (OB_FAIL(reader.read_i32(value))) {
                  break;
                }
                num_page_values = value;
              } else if (2 == sub_id && THRIFT_I32 == sub_type) {
                int32_t value = 0;
                if (OB_FAIL(reader.read_i32(value))) {
                  break;
                }
                encoding = value;
              } else if (3 == sub_id && THRIFT_I32 == sub_type) {
                int32_t value = 0;
                if (OB_FAIL(reader.read_i32(value))) {
                  break;
                }
                def_level_encoding = value;
              } else if (4 == sub_id && THRIFT_I32 == sub_type) {
                int32_t value = 0;
                if (OB_FAIL(reader.read_i32(value))) {
                  break;
                }
                rep_level_encoding = value;
              } else if (OB_FAIL(reader.skip(sub_type))) {
                break;
              }
            }
            reader.pop_struct();
          } else if (7 == field_id && THRIFT_STRUCT == field_type) {
            // the dictionary page header
            if (OB_FAIL(reader.push_struct())) {
              break;
            }
            while (true) {
              int32_t sub_type = 0;
              int32_t sub_id = 0;
              bool sub_stop = false;
              if (OB_FAIL(reader.read_field_header(sub_type, sub_id, sub_stop))) {
                break;
              }
              if (sub_stop) {
                break;
              }
              if (1 == sub_id && THRIFT_I32 == sub_type) {
                int32_t value = 0;
                if (OB_FAIL(reader.read_i32(value))) {
                  break;
                }
                num_page_values = value;
              } else if (OB_FAIL(reader.skip(sub_type))) {
                break;
              }
            }
            reader.pop_struct();
          } else if (8 == field_id && THRIFT_STRUCT == field_type) {
            // the data page header of the version two pages

            if (OB_FAIL(reader.push_struct())) {
              break;
            }
            while (true) {
              int32_t sub_type = 0;
              int32_t sub_id = 0;
              bool sub_stop = false;
              if (OB_FAIL(reader.read_field_header(sub_type, sub_id, sub_stop))) {
                break;
              }
              if (sub_stop) {
                break;
              }
              if (1 == sub_id && THRIFT_I32 == sub_type) {
                int32_t value = 0;
                if (OB_FAIL(reader.read_i32(value))) {
                  break;
                }
                num_page_values = value;
              } else if (4 == sub_id && THRIFT_I32 == sub_type) {
                int32_t value = 0;
                if (OB_FAIL(reader.read_i32(value))) {
                  break;
                }
                encoding = value;
              } else if (5 == sub_id && THRIFT_I32 == sub_type) {
                int32_t value = 0;
                if (OB_FAIL(reader.read_i32(value))) {
                  break;
                }
                def_levels_byte_length = value;
              } else if (6 == sub_id && THRIFT_I32 == sub_type) {
                int32_t value = 0;
                if (OB_FAIL(reader.read_i32(value))) {
                  break;
                }
                rep_levels_byte_length = value;
              } else if (7 == sub_id && THRIFT_BOOLEAN_TRUE == sub_type) {
                is_compressed = true;
              } else if (7 == sub_id && THRIFT_BOOLEAN_FALSE == sub_type) {
                is_compressed = false;
              } else if (OB_FAIL(reader.skip(sub_type))) {
                break;
              }
            }
            reader.pop_struct();
          } else if (OB_FAIL(reader.skip(field_type))) {
            break;
          }
        }
        reader.pop_struct();

        const int64_t page_data_offset = offset + reader.get_pos();
        std::string compressed;
        compressed.resize(compressed_size);
        if (compressed_size > 0) {
          if (OB_FAIL(source_->read(page_data_offset, compressed_size, &compressed[0]))) {
            break;
          }
        }
        offset = page_data_offset + compressed_size;

        if (PAGE_DATA_V2 == page_type) {
          // the repetition and the definition levels of the version two pages are never
          // compressed, only the values are
          const int64_t levels_length = rep_levels_byte_length + def_levels_byte_length;
          if (compressed_size < levels_length || uncompressed_size < levels_length) {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          std::string value_buffer;
          const int64_t compressed_value_length = compressed_size - levels_length;
          const int64_t value_length = uncompressed_size - levels_length;
          if (ObParquetCodec::UNCOMPRESSED == chunk.codec_ || !is_compressed) {
            value_buffer.assign(compressed, levels_length, compressed_value_length);
          } else if (ObParquetCodec::SNAPPY == chunk.codec_) {
            value_buffer.resize(value_length);
            if (OB_FAIL(snappy_decompress(compressed.data() + levels_length,
                                           compressed_value_length,
                                           &value_buffer[0],
                                           value_length))) {
              break;
            }
          } else {
            ret = OB_NOT_SUPPORTED;
            break;
          }
          std::vector<int32_t> levels;
          if (def_levels_byte_length > 0) {
            if (OB_FAIL(decode_rle_levels(compressed.data() + rep_levels_byte_length,
                                          def_levels_byte_length,
                                          num_page_values,
                                          bit_width_of(column.max_def_level_),
                                          levels))) {
              break;
            }
          }
          std::vector<ObParquetValue> page_values;
          const int64_t non_null = (column.max_def_level_ > 0) ? static_cast<int64_t>(levels.size())
                                                                : num_page_values;
          if (OB_FAIL(decode_plain(value_buffer.data(),
                                   value_buffer.size(),
                                   non_null,
                                   chunk.type_,
                                   page_values))) {
            break;
          }
          size_t value_index = 0;
          for (int64_t i = 0; i < num_page_values; ++i) {
            ObParquetValue value;
            value.type_ = chunk.type_;
            const int32_t level =
                (i < static_cast<int64_t>(levels.size())) ? levels[i] : column.max_def_level_;
            if (column.max_def_level_ > 0 && level < column.max_def_level_) {
              value.is_null_ = true;
            } else if (value_index < page_values.size()) {
              value = page_values[value_index++];
            }
            values.push_back(value);
          }
          num_values += num_page_values;
          continue;
        }

        std::string page;
        if (ObParquetCodec::UNCOMPRESSED == chunk.codec_) {
          page.swap(compressed);
        } else if (ObParquetCodec::SNAPPY == chunk.codec_) {
          page.resize(uncompressed_size);
          if (OB_FAIL(snappy_decompress(compressed.data(), compressed.size(), &page[0], uncompressed_size))) {
            break;
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          break;
        }

        if (PAGE_DICTIONARY == page_type) {
          // decode the dictionary into the plain representation
          std::vector<ObParquetValue> dict_values;
          if (OB_FAIL(decode_plain(page.data(), page.size(), num_page_values, chunk.type_, dict_values))) {
            break;
          }
          dictionary.clear();
          for (size_t i = 0; i < dict_values.size(); ++i) {
            if (dict_values[i].bytes_ptr_ != NULL) {
              dictionary.append(dict_values[i].bytes_ptr_, dict_values[i].bytes_len_);
            } else {
              dictionary.append(reinterpret_cast<const char *>(&dict_values[i].int_value_),
                                sizeof(int64_t));
            }
          }
        } else if (PAGE_DATA == page_type) {
          if (OB_FAIL(decode_data_page(page.data(),
                                       page.size(),
                                       num_page_values,
                                       column.max_def_level_,
                                       chunk.type_,
                                       static_cast<ObParquetEncoding>(encoding),
                                       values,
                                       dictionary))) {
            break;
          }
          num_values += num_page_values;
        }
      }
    }
  }
  return ret;
}

int ObFileParquetReader::decode_data_page(const char *data,
                                          int64_t length,
                                          int64_t num_values,
                                          int32_t max_def_level,
                                          ObParquetType type,
                                          ObParquetEncoding encoding,
                                          std::vector<ObParquetValue> &values,
                                          std::string &dictionary)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  std::vector<int32_t> levels;
  if (max_def_level > 0) {
    // the definition levels of the version one pages are prefixed with their length
    if (length - pos < 4) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      uint32_t level_bytes = 0;
      for (int i = 0; i < 4; ++i) {
        level_bytes |= static_cast<uint32_t>(static_cast<uint8_t>(data[pos + i])) << (8 * i);
      }
      pos += 4;
      if (pos + static_cast<int64_t>(level_bytes) > length) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(decode_rle_levels(data + pos,
                                          level_bytes,
                                          num_values,
                                          bit_width_of(max_def_level),
                                          levels))) {
      } else {
        pos += level_bytes;
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t non_null = (max_def_level > 0) ? static_cast<int64_t>(levels.size()) : num_values;
    std::vector<ObParquetValue> page_values;
    if (ObParquetEncoding::PLAIN == encoding) {
      if (OB_FAIL(decode_plain(data + pos, length - pos, non_null, type, page_values))) {
        return ret;
      }
    } else if (ObParquetEncoding::PLAIN_DICTIONARY == encoding
               || ObParquetEncoding::RLE_DICTIONARY == encoding) {
      if (OB_FAIL(decode_dictionary(data + pos, length - pos, non_null, type, dictionary, page_values))) {
        return ret;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      return ret;
    }
    size_t value_index = 0;
    for (int64_t i = 0; i < num_values; ++i) {
      ObParquetValue value;
      value.type_ = type;
      if (max_def_level > 0) {
        const int32_t level = (i < static_cast<int64_t>(levels.size())) ? levels[i] : max_def_level;
        if (level < max_def_level) {
          value.is_null_ = true;
          values.push_back(value);
          continue;
        }
      }
      if (value_index < page_values.size()) {
        value = page_values[value_index++];
      }
      values.push_back(value);
    }
  }
  return ret;
}

int ObFileParquetReader::decode_plain(const char *data,
                                      int64_t length,
                                      int64_t count,
                                      ObParquetType type,
                                      std::vector<ObParquetValue> &values)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  // a negative count means that the values are decoded until the data is exhausted
  const bool until_the_end = (count < 0);
  switch (type) {
  case ObParquetType::BOOLEAN: {
    // the booleans are bit packed
    for (int64_t i = 0; i < count; ++i) {
      const int64_t byte_index = i / 8;
      if (byte_index >= length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      ObParquetValue value;
      value.type_ = type;
      value.int_value_ = ((static_cast<uint8_t>(data[byte_index]) >> (i % 8)) & 1) ? 1 : 0;
      values.push_back(value);
    }
    break;
  }
  case ObParquetType::INT32: {
    if (length < count * 4) {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    for (int64_t i = 0; i < count; ++i) {
      uint32_t raw = 0;
      for (int j = 0; j < 4; ++j) {
        raw |= static_cast<uint32_t>(static_cast<uint8_t>(data[pos + j])) << (8 * j);
      }
      pos += 4;
      ObParquetValue value;
      value.type_ = type;
      value.int_value_ = static_cast<int32_t>(raw);
      values.push_back(value);
    }
    break;
  }
  case ObParquetType::INT64: {
    if (length < count * 8) {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    for (int64_t i = 0; i < count; ++i) {
      uint64_t raw = 0;
      for (int j = 0; j < 8; ++j) {
        raw |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos + j])) << (8 * j);
      }
      pos += 8;
      ObParquetValue value;
      value.type_ = type;
      value.int_value_ = static_cast<int64_t>(raw);
      values.push_back(value);
    }
    break;
  }
  case ObParquetType::FLOAT: {
    if (length < count * 4) {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    for (int64_t i = 0; i < count; ++i) {
      uint32_t raw = 0;
      for (int j = 0; j < 4; ++j) {
        raw |= static_cast<uint32_t>(static_cast<uint8_t>(data[pos + j])) << (8 * j);
      }
      pos += 4;
      float single = 0.0f;
      memcpy(&single, &raw, sizeof(single));
      ObParquetValue value;
      value.type_ = type;
      value.double_value_ = static_cast<double>(single);
      values.push_back(value);
    }
    break;
  }
  case ObParquetType::DOUBLE: {
    if (length < count * 8) {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    for (int64_t i = 0; i < count; ++i) {
      uint64_t raw = 0;
      for (int j = 0; j < 8; ++j) {
        raw |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos + j])) << (8 * j);
      }
      pos += 8;
      double single = 0.0;
      memcpy(&single, &raw, sizeof(single));
      ObParquetValue value;
      value.type_ = type;
      value.double_value_ = single;
      values.push_back(value);
    }
    break;
  }
  case ObParquetType::BYTE_ARRAY:
  case ObParquetType::FIXED_LEN_BYTE_ARRAY: {
    for (int64_t i = 0; until_the_end ? (pos < length) : (i < count); ++i) {
      int32_t item_length = 0;
      if (ObParquetType::BYTE_ARRAY == type) {
        if (length - pos < 4) {
          if (until_the_end) {
            break;
          }
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        uint32_t raw = 0;
        for (int j = 0; j < 4; ++j) {
          raw |= static_cast<uint32_t>(static_cast<uint8_t>(data[pos + j])) << (8 * j);
        }
        pos += 4;
        item_length = static_cast<int32_t>(raw);
      } else {
        item_length = 0;
      }
      if (length - pos < item_length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      ObParquetValue value;
      value.type_ = type;
      value.bytes_ptr_ = data + pos;
      value.bytes_len_ = item_length;
      pos += item_length;
      values.push_back(value);
    }
    break;
  }
  case ObParquetType::INT96:
  default:
    ret = OB_NOT_SUPPORTED;
    break;
  }
  return ret;
}

int ObFileParquetReader::decode_dictionary(const char *data,
                                           int64_t length,
                                           int64_t count,
                                           ObParquetType type,
                                           const std::string &dictionary,
                                           std::vector<ObParquetValue> &values)
{
  int ret = OB_SUCCESS;
  if (length < 1) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int32_t bit_width = static_cast<int8_t>(data[0]);
    std::vector<int32_t> indices;
    if (OB_FAIL(decode_rle_levels(data + 1, length - 1, count, bit_width, indices))) {
      return ret;
    }
    // rebuild the dictionary
    std::vector<ObParquetValue> dict_values;
    if (OB_FAIL(decode_plain(dictionary.data(), dictionary.size(), -1, type, dict_values))) {
      return ret;
    }
    for (size_t i = 0; i < indices.size(); ++i) {
      ObParquetValue value;
      value.type_ = type;
      const int32_t index = indices[i];
      if (index >= 0 && index < static_cast<int32_t>(dict_values.size())) {
        value = dict_values[index];
      }
      values.push_back(value);
    }
  }
  return ret;
}

int ObFileParquetReader::decode_rle_levels(const char *data,
                                           int64_t length,
                                           int64_t count,
                                           int32_t bit_width,
                                           std::vector<int32_t> &levels)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (0 == bit_width) {
    levels.assign(count, 0);
    return ret;
  }
  const int32_t byte_width = (bit_width + 7) / 8;
  while (static_cast<int64_t>(levels.size()) < count && pos < length) {
    // the run header
    uint64_t header = 0;
    int shift = 0;
    while (pos < length) {
      const uint8_t byte = static_cast<uint8_t>(data[pos++]);
      header |= static_cast<uint64_t>(byte & 0x7F) << shift;
      if (0 == (byte & 0x80)) {
        break;
      }
      shift += 7;
      if (shift > 63) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
    }
    if (OB_FAIL(ret)) {
      break;
    }
    if (0 == (header & 1)) {
      // a run of identical values
      const int64_t run_length = static_cast<int64_t>(header >> 1);
      if (pos + byte_width > length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      int32_t value = 0;
      for (int i = 0; i < byte_width; ++i) {
        value |= static_cast<int32_t>(static_cast<uint8_t>(data[pos + i])) << (8 * i);
      }
      pos += byte_width;
      for (int64_t i = 0; i < run_length && static_cast<int64_t>(levels.size()) < count; ++i) {
        levels.push_back(value);
      }
    } else {
      // a bit packed run
      const int64_t num_groups = static_cast<int64_t>(header >> 1);
      const int64_t num_bytes = num_groups * bit_width;
      if (pos + num_bytes > length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      int64_t bit_pos = 0;
      for (int64_t g = 0; g < num_groups * 8 && static_cast<int64_t>(levels.size()) < count; ++g) {
        int32_t value = 0;
        for (int b = 0; b < bit_width; ++b) {
          const int64_t byte_index = (bit_pos + b) / 8;
          const int32_t bit_index = static_cast<int32_t>((bit_pos + b) % 8);
          const uint8_t byte = (byte_index < num_bytes) ? static_cast<uint8_t>(data[pos + byte_index]) : 0;
          value |= static_cast<int32_t>((byte >> bit_index) & 1) << b;
        }
        bit_pos += bit_width;
        levels.push_back(value);
      }
      pos += num_bytes;
    }
  }
  return ret;
}

/* ------------------------------------------------------------------------- */
/* snappy                                                                    */
/* ------------------------------------------------------------------------- */

int ObFileParquetReader::snappy_decompress(const char *input,
                                           int64_t input_length,
                                           char *output,
                                           int64_t output_length)
{
  int ret = OB_SUCCESS;
  int64_t in_pos = 0;
  int64_t out_pos = 0;
  // the uncompressed length is stored as a varint
  uint64_t expected = 0;
  int shift = 0;
  while (in_pos < input_length) {
    const uint8_t byte = static_cast<uint8_t>(input[in_pos++]);
    expected |= static_cast<uint64_t>(byte & 0x7F) << shift;
    if (0 == (byte & 0x80)) {
      break;
    }
    shift += 7;
    if (shift > 63) {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  if (OB_SUCC(ret) && static_cast<int64_t>(expected) != output_length) {
    ret = OB_ERR_UNEXPECTED;
  }
  while (OB_SUCC(ret) && in_pos < input_length) {
    const uint8_t tag = static_cast<uint8_t>(input[in_pos++]);
    const int32_t type = tag & 0x03;
    if (0 == type) {
      // a literal
      int64_t literal_length = (tag >> 2) + 1;
      if (literal_length > 60) {
        const int64_t extra = literal_length - 60;
        if (in_pos + extra > input_length) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        uint64_t raw = 0;
        for (int64_t i = 0; i < extra; ++i) {
          raw |= static_cast<uint64_t>(static_cast<uint8_t>(input[in_pos + i])) << (8 * i);
        }
        in_pos += extra;
        literal_length = static_cast<int64_t>(raw) + 1;
      }
      if (in_pos + literal_length > input_length || out_pos + literal_length > output_length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      memcpy(output + out_pos, input + in_pos, static_cast<size_t>(literal_length));
      in_pos += literal_length;
      out_pos += literal_length;
    } else {
      // a copy
      int64_t copy_length = 0;
      int64_t offset = 0;
      if (1 == type) {
        if (in_pos >= input_length) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        copy_length = 4 + ((tag >> 2) & 0x07);
        offset = (static_cast<int64_t>(tag & 0xE0) << 3) | static_cast<uint8_t>(input[in_pos]);
        in_pos += 1;
      } else if (2 == type) {
        if (in_pos + 2 > input_length) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        copy_length = (tag >> 2) + 1;
        offset = static_cast<uint8_t>(input[in_pos]) | (static_cast<int64_t>(static_cast<uint8_t>(input[in_pos + 1])) << 8);
        in_pos += 2;
      } else {
        if (in_pos + 4 > input_length) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        copy_length = (tag >> 2) + 1;
        offset = 0;
        for (int i = 0; i < 4; ++i) {
          offset |= static_cast<int64_t>(static_cast<uint8_t>(input[in_pos + i])) << (8 * i);
        }
        in_pos += 4;
      }
      if (offset <= 0 || offset > out_pos || out_pos + copy_length > output_length) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      // the copy may overlap
      for (int64_t i = 0; i < copy_length; ++i) {
        output[out_pos + i] = output[out_pos - offset + i];
      }
      out_pos += copy_length;
    }
  }
  if (OB_SUCC(ret) && out_pos != output_length) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
