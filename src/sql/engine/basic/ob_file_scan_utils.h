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

#include <deque>
#include <string>
#include <utility>
#include <vector>

namespace oceanbase
{
namespace sql
{

/**
 * @brief The physical file format handled by the file table functions.
 */
enum class ObFileScanFormat
{
  CSV = 0,
  JSONL,
  PARQUET,
  GLOB
};

/**
 * @brief Column types produced by the deterministic type inferencer.
 *
 * The seven supported types are NULL, BIGINT, DOUBLE, BOOLEAN, VARCHAR, DATE
 * and DATETIME.  NULL means that every observed value of the column was NULL.
 */
enum class ObFileScanType
{
  UNKNOWN = 0, ///< every observed value was NULL
  BIGINT,
  DOUBLE,
  BOOLEAN,
  VARCHAR,
  DATE,
  DATETIME
};

/**
 * @brief Return the display name of an inferred column type.
 *
 * The spelling follows the DuckDB style used by the ``DESCRIBE`` output.
 */
const char *ob_file_scan_type_name(ObFileScanType type);

/**
 * @brief One column of an inferred file schema.
 */
struct ObFileScanColumn
{
  common::ObString name_;
  ObFileScanType type_;
  bool nullable_;

  ObFileScanColumn() : name_(), type_(ObFileScanType::UNKNOWN), nullable_(false) {}
};

/**
 * @brief Options shared by all file table functions.
 */
struct ObFileScanOptions
{
  common::ObString path_;
  ObFileScanFormat format_;
  bool has_header_;     ///< the ``header`` option was given explicitly
  bool header_;         ///< the first record of a CSV file holds the column names
  int64_t sample_size_; ///< only ``-1`` (inspect the whole file) is supported
  common::ObString null_str_;
  common::ObString delimiter_;
  common::ObString quote_;
  common::ObString escape_;
  common::ObString json_format_;
  bool ignore_errors_;
  int64_t max_field_size_;
  int64_t max_record_size_;

  ObFileScanOptions()
      : path_(),
        format_(ObFileScanFormat::CSV),
        has_header_(false),
        header_(true),
        sample_size_(-1),
        null_str_(),
        delimiter_(","),
        quote_("\""),
        escape_(),
        json_format_(),
        ignore_errors_(false),
        max_field_size_(16 * 1024 * 1024),
        max_record_size_(64 * 1024 * 1024)
  {}
};

/**
 * @brief The kind of a value read from a file.
 *
 * The kind tells the type inferencer how the value has to be interpreted, a
 * plain text value is classified by its content while a json value already
 * carries its type.
 */
enum class ObFileScanValueKind
{
  TEXT = 0, ///< a plain text value, the type is inferred from the content
  JSON_STRING,
  JSON_INT,
  JSON_DOUBLE,
  JSON_BOOL,
  JSON_NULL
};

/**
 * @brief One field of a record read from a file.
 */
struct ObFileScanValue
{
  common::ObString value_;
  bool is_null_;
  ObFileScanValueKind kind_;

  ObFileScanValue() : value_(), is_null_(false), kind_(ObFileScanValueKind::TEXT) {}
  ObFileScanValue(const common::ObString &value,
                  bool is_null,
                  ObFileScanValueKind kind = ObFileScanValueKind::TEXT)
      : value_(value), is_null_(is_null), kind_(kind)
  {}
};

/**
 * @brief The sink that receives the rows produced by a format reader.
 *
 * The values handed to the sink are only valid for the duration of the call,
 * the readers may reuse their internal buffers.
 */
class ObFileScanSink
{
public:
  virtual ~ObFileScanSink() {}
  /**
   * @brief Called once for every record read from the file.
   *
   * @param values the fields of the record, indexed by column
   * @param count  the number of columns
   */
  virtual int on_row(const ObFileScanValue *values, int64_t count) = 0;
};

/**
 * @brief The interface of the readers of the file formats.
 */
class ObFileScanReader
{
public:
  virtual ~ObFileScanReader() {}
  /**
   * @brief Feed a chunk of the file to the reader.
   *
   * @param data   the chunk
   * @param length the length of the chunk
   * @param is_end whether this is the last chunk of the file
   * @param sink   the sink that receives the records
   */
  virtual int feed(const char *data, int64_t length, bool is_end, ObFileScanSink &sink) = 0;
};

/**
 * @brief A streaming CSV reader.
 *
 * The reader never materializes the whole file: the caller feeds it with
 * successive chunks of the file and the reader emits the records that could be
 * completed with the data seen so far.
 */
class ObFileCsvReader : public ObFileScanReader
{
public:
  ObFileCsvReader();
  ~ObFileCsvReader();

  /**
   * @brief Prepare the reader.
   *
   * @param options              the reader options
   * @param skip_header          the first record holds the column names and is not emitted
   * @param expected_field_count the number of columns, 0 means "learn it from the header"
   */
  int init(const ObFileScanOptions &options, bool skip_header, int64_t expected_field_count);

  /**
   * @brief Feed a chunk of the file to the reader.
   *
   * @param data   the chunk
   * @param length the length of the chunk
   * @param is_end whether this is the last chunk of the file
   * @param sink   the sink that receives the records
   */
  virtual int feed(const char *data, int64_t length, bool is_end, ObFileScanSink &sink) override;

  int64_t get_record_count() const { return record_count_; }
  int64_t get_line_number() const { return line_number_; }
  /// the column names read from the header record, empty when the file has no header
  const std::vector<common::ObString> &get_header() const { return header_; }
  bool has_header() const { return header_seen_; }

private:
  int scan_records(bool is_end, ObFileScanSink &sink);
  int parse_record(int64_t &cursor, bool is_end, bool &record_complete);
  int parse_field(int64_t &cursor, bool is_end, bool &field_complete);
  int emit_record(ObFileScanSink &sink);
  int check_field_count(int64_t count);

private:
  ObFileScanOptions options_;
  bool skip_header_;
  int64_t expected_field_count_;
  int64_t field_count_;
  bool header_seen_;
  bool header_record_;
  int64_t record_count_;
  int64_t line_number_;
  std::vector<char> buf_;
  int64_t head_;
  int64_t tail_;
  std::string scratch_;
  std::vector<ObFileScanValue> values_;
  std::vector<char> field_raw_;
  std::vector<const char *> field_raw_ptr_;
  std::vector<int64_t> field_off_;
  std::vector<int32_t> field_len_;
  std::vector<char> null_flags_;
  std::vector<common::ObString> header_;
  std::string header_storage_;
};

/**
 * @brief A streaming reader for newline delimited JSON objects.
 *
 * Only flat objects are supported, nested objects and arrays are rejected.
 * The reader keeps the union of all field names in order of first appearance.
 */
class ObFileJsonReader : public ObFileScanReader
{
public:
  ObFileJsonReader();
  ~ObFileJsonReader();

  /**
   * @brief Prepare the reader.
   *
   * @param options       the reader options
   * @param expected_names when not empty the field names are mapped onto this
   *                      list, otherwise the union of the keys is collected
   */
  int init(const ObFileScanOptions &options, const std::vector<common::ObString> *expected_names);

  /**
   * @brief Feed a chunk of the file to the reader.
   */
  virtual int feed(const char *data, int64_t length, bool is_end, ObFileScanSink &sink) override;

  const std::vector<common::ObString> &get_field_names() const { return field_names_; }
  int64_t get_record_count() const { return record_count_; }

private:
  int scan_records(bool is_end, ObFileScanSink &sink);
  int parse_object(int64_t begin, int64_t end, ObFileScanSink &sink);
  int skip_ws(int64_t &pos, int64_t end);
  int parse_string(int64_t &pos, int64_t end, std::string &out);
  int parse_number(int64_t &pos, int64_t end, bool &is_integer, std::string &out);
  int add_field_name(const common::ObString &name, int64_t &index);

private:
  ObFileScanOptions options_;
  bool collect_names_;
  std::vector<common::ObString> field_names_;
  std::vector<common::ObString> expected_names_;
  // a stable storage for the field names, the deque never moves its elements
  std::deque<std::string> name_pool_;
  int64_t record_count_;
  std::vector<char> buf_;
  int64_t head_;
  int64_t tail_;
  std::string scratch_;
  std::vector<ObFileScanValue> values_;
};

/**
 * @brief Accumulates the type of a column while the file is inspected.
 */
class ObFileTypeInferer{
public:
  ObFileTypeInferer() : type_(ObFileScanType::UNKNOWN), nullable_(false), has_value_(false) {}

  /// Observe a NULL value.
  void observe_null() { nullable_ = true; }
  /// Observe the text of a value read from a CSV file.
  void observe_text(const common::ObString &text);
  /// Observe a JSON string value.
  void observe_json_string(const common::ObString &text);
  /// Observe a JSON integer value.
  void observe_json_int();
  /// Observe a JSON floating point value.
  void observe_json_double();
  /// Observe a JSON boolean value.
  void observe_json_bool();
  /// Merge the observations of another inferer into this one.
  void merge(const ObFileTypeInferer &other);

  ObFileScanType get_type() const { return type_; }
  bool is_nullable() const { return nullable_; }
  bool has_value() const { return has_value_; }

private:
  void merge_type(ObFileScanType other);

private:
  ObFileScanType type_;
  bool nullable_;
  bool has_value_;
};

/**
 * @brief Infers the schema of a file while it is read.
 *
 * The inferer is used as the sink of the format readers.  It keeps track of the
 * type of every column and of the number of records that actually provided a
 * value for the column, a column is nullable when one of the records does not
 * provide a value for it or when one of the values is NULL.
 */
class ObFileSchemaInferer : public ObFileScanSink
{
public:
  ObFileSchemaInferer() : row_count_(0) {}
  int on_row(const ObFileScanValue *values, int64_t count) override;

  int64_t get_row_count() const { return row_count_; }
  const std::vector<ObFileTypeInferer> &get_inferers() const { return inferers_; }

  /**
   * @brief Build the final column list.
   *
   * @param names   the column names
   * @param columns the resulting columns
   */
  int build_columns(const std::vector<common::ObString> &names,
                    std::vector<ObFileScanColumn> &columns) const;

private:
  int64_t row_count_;
  std::vector<ObFileTypeInferer> inferers_;
  std::vector<int64_t> present_counts_;
};

/**
 * @brief Merge two inferred types using the widening rules.
 */
ObFileScanType ob_file_scan_merge_type(ObFileScanType lhs, ObFileScanType rhs);
/**
 * @brief Check whether the text looks like an integer that fits into int64.
 */
bool ob_file_scan_try_parse_int64(const char *data, int64_t length, int64_t &value);

/**
 * @brief Check whether the text looks like a floating point number.
 */
bool ob_file_scan_try_parse_double(const char *data, int64_t length, double &value);

/**
 * @brief Check whether the text is a boolean literal.
 */
bool ob_file_scan_is_bool_text(const char *data, int64_t length);

/**
 * @brief Check whether the text is a valid ``YYYY-MM-DD`` date.
 */
bool ob_file_scan_is_date_text(const char *data, int64_t length);

/**
 * @brief Check whether the text is a valid date time literal.
 *
 * Both ``YYYY-MM-DD hh:mm:ss`` and the ISO 8601 ``YYYY-MM-DDThh:mm:ss`` spelling
 * are accepted, the fractional seconds are optional.
 */
bool ob_file_scan_is_datetime_text(const char *data, int64_t length);

/**
 * @brief Normalize a date time literal so that it can be handed to the
 *        OceanBase date time converter.
 *
 * The ISO 8601 ``T`` separator is replaced by a space and a missing time part
 * is filled with zeroes.
 */
void ob_file_scan_normalize_datetime(const common::ObString &text, std::string &out);

/**
 * @brief Check whether the day is valid for the given year and month.
 */
bool ob_file_scan_is_valid_date(int64_t year, int64_t month, int64_t day);

} // namespace sql
} // namespace oceanbase
