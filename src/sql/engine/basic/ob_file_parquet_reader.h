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

#pragma once

#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"

#include <string>
#include <vector>

namespace oceanbase
{
namespace sql
{

/**
 * @brief The physical types of the parquet format.
 */
enum class ObParquetType
{
  BOOLEAN = 0,
  INT32 = 1,
  INT64 = 2,
  INT96 = 3,
  FLOAT = 4,
  DOUBLE = 5,
  BYTE_ARRAY = 6,
  FIXED_LEN_BYTE_ARRAY = 7
};

/**
 * @brief The repetition types of the parquet format.
 */
enum class ObParquetRepetition
{
  REQUIRED = 0,
  OPTIONAL = 1,
  REPEATED = 2
};

/**
 * @brief The compression codecs of the parquet format.
 */
enum class ObParquetCodec
{
  UNCOMPRESSED = 0,
  SNAPPY = 1,
  GZIP = 2,
  LZO = 3,
  BROTLI = 4,
  LZ4 = 5,
  ZSTD = 6,
  LZ4_RAW = 7
};

/**
 * @brief The encodings of the parquet format.
 */
enum class ObParquetEncoding
{
  PLAIN = 0,
  PLAIN_DICTIONARY = 2,
  RLE = 3,
  BIT_PACKED = 4,
  DELTA_BINARY_PACKED = 5,
  DELTA_LENGTH_BYTE_ARRAY = 6,
  DELTA_BYTE_ARRAY = 7,
  RLE_DICTIONARY = 8
};

/**
 * @brief The description of one leaf column of a parquet file.
 */
struct ObParquetColumnMeta
{
  std::string name_;
  ObParquetType type_;
  int32_t type_length_;
  ObParquetRepetition repetition_type_;
  int32_t converted_type_;
  int32_t max_def_level_;
  int32_t max_rep_level_;

  ObParquetColumnMeta()
      : type_(ObParquetType::INT64),
        type_length_(0),
        repetition_type_(ObParquetRepetition::OPTIONAL),
        converted_type_(-1),
        max_def_level_(0),
        max_rep_level_(0)
  {}
};

/**
 * @brief The description of one column chunk of a row group.
 */
struct ObParquetColumnChunkMeta
{
  int64_t file_offset_;
  int64_t data_page_offset_;
  int64_t dictionary_page_offset_;
  int64_t total_compressed_size_;
  int64_t total_uncompressed_size_;
  int64_t num_values_;
  ObParquetCodec codec_;
  ObParquetType type_;

  ObParquetColumnChunkMeta()
      : file_offset_(-1),
        data_page_offset_(-1),
        dictionary_page_offset_(-1),
        total_compressed_size_(0),
        total_uncompressed_size_(0),
        num_values_(0),
        codec_(ObParquetCodec::UNCOMPRESSED),
        type_(ObParquetType::INT64)
  {}
};

/**
 * @brief The description of one row group of a parquet file.
 */
struct ObParquetRowGroupMeta
{
  int64_t num_rows_;
  int64_t total_byte_size_;
  std::vector<ObParquetColumnChunkMeta> chunks_;

  ObParquetRowGroupMeta() : num_rows_(0), total_byte_size_(0) {}
};

/**
 * @brief One value of a parquet column.
 */
struct ObParquetValue
{
  ObParquetType type_;
  bool is_null_;
  int64_t int_value_;
  double double_value_;
  const char *bytes_ptr_;
  int32_t bytes_len_;

  ObParquetValue()
      : type_(ObParquetType::INT64), is_null_(false), int_value_(0), double_value_(0.0), bytes_ptr_(NULL),
        bytes_len_(0)
  {}
};

/**
 * @brief The byte source of a parquet file.
 *
 * The reader only reads the byte ranges it needs, the file is never
 * materialized in memory as a whole.
 */
class ObParquetFileSource
{
public:
  virtual ~ObParquetFileSource() {}
  /// the size of the file
  virtual int64_t get_size() const = 0;
  /// read ``length`` bytes at ``offset`` into ``buf``
  virtual int read(int64_t offset, int64_t length, char *buf) = 0;
};

/**
 * @brief A parquet file source that is backed by a memory buffer.
 */
class ObParquetMemorySource : public ObParquetFileSource
{
public:
  ObParquetMemorySource() : data_(NULL), length_(0) {}
  int init(const char *data, int64_t length)
  {
    data_ = data;
    length_ = length;
    return OB_SUCCESS;
  }
  virtual int64_t get_size() const override { return length_; }
  virtual int read(int64_t offset, int64_t length, char *buf) override;

private:
  const char *data_;
  int64_t length_;
};

/**
 * @brief A reader for the parquet format.
 *
 * Only the subset of the format that is needed by the file table functions is
 * implemented: a flat schema, the plain and the dictionary encodings, the RLE
 * definition levels and the uncompressed and the snappy compressed data pages.
 */
class ObFileParquetReader
{
public:
  ObFileParquetReader();
  ~ObFileParquetReader();

  /**
   * @brief Open the file and read its footer.
   */
  int init(ObParquetFileSource *source);

  const std::vector<ObParquetColumnMeta> &get_columns() const { return columns_; }
  const std::vector<ObParquetRowGroupMeta> &get_row_groups() const { return row_groups_; }
  int64_t get_num_rows() const { return num_rows_; }

  /**
   * @brief Read the values of one column of one row group.
   *
   * The values are appended to ``values``, one entry per row of the row group.
   */
  int read_column(int64_t row_group_index,
                  int64_t column_index,
                  std::vector<ObParquetValue> &values);

  /**
   * @brief Decompress a snappy compressed buffer.
   */
  static int snappy_decompress(const char *input, int64_t input_length, char *output, int64_t output_length);

private:
  int read_footer();
  int parse_file_metadata(const char *data, int64_t length);
  int decode_data_page(const char *data,
                       int64_t length,
                       int64_t num_values,
                       int32_t max_def_level,
                       ObParquetType type,
                       ObParquetEncoding encoding,
                       std::vector<ObParquetValue> &values,
                       std::string &dictionary);
  int decode_plain(const char *data,
                   int64_t length,
                   int64_t count,
                   ObParquetType type,
                   std::vector<ObParquetValue> &values);
  int decode_dictionary(const char *data,
                        int64_t length,
                        int64_t count,
                        ObParquetType type,
                        const std::string &dictionary,
                        std::vector<ObParquetValue> &values);
  int decode_rle_levels(const char *data,
                        int64_t length,
                        int64_t count,
                        int32_t bit_width,
                        std::vector<int32_t> &levels);

private:
  ObParquetFileSource *source_;
  std::vector<ObParquetColumnMeta> columns_;
  std::vector<ObParquetRowGroupMeta> row_groups_;
  int64_t num_rows_;
};

} // namespace sql
} // namespace oceanbase
