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

#include "sql/engine/basic/ob_file_table_access_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "common/timezone/ob_time_convert.h"

namespace oceanbase
{
namespace sql
{

namespace
{
/// the size of the chunk that is read from a file
const int64_t FILE_TABLE_READ_CHUNK_SIZE = 16 * 1024;
} // namespace

// The unified serialization of the specification has to be defined, otherwise
// the class keeps an undefined key function and its vtable never reaches the
// binary.
OB_SERIALIZE_MEMBER_INHERIT(ObFileTableAccessSpec,
                            ObOpSpec,
                            column_exprs_,
                            column_types_,
                            column_names_,
                            path_,
                            null_str_,
                            delimiter_,
                            quote_,
                            escape_,
                            format_,
                            header_,
                            file_list_);

ObFileTableAccessOp::ObFileTableAccessOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      allocator_(&exec_ctx.get_allocator()),
      file_source_(NULL),
      file_offset_(0),
      file_size_(0),
      is_file_opened_(false),
      is_file_end_(false),
      pending_row_index_(0),
      pending_row_count_(0),
      file_index_(0),
      row_group_index_(0),
      row_index_in_group_(0),
      row_count_in_group_(0),
      is_eof_(false),
      is_opened_(false)
{}

ObFileTableAccessOp::~ObFileTableAccessOp() {}

int ObFileTableAccessOp::on_row(const ObFileScanValue *values, int64_t count)
{
  int ret = OB_SUCCESS;
  // The strings of a record are only valid during the call, the reader reuses
  // its buffers for the next record. The values are copied so that the rows
  // that wait for the consumer stay valid.
  for (int64_t i = 0; i < count; ++i) {
    if (values[i].is_null_ || values[i].value_.empty()) {
      pending_strings_.push_back(std::string());
    } else {
      pending_strings_.push_back(std::string(values[i].value_.ptr(), values[i].value_.length()));
    }
  }
  for (int64_t i = 0; i < count; ++i) {
    pending_values_.push_back(values[i]);
  }
  pending_row_ends_.push_back(static_cast<int64_t>(pending_values_.size()));
  ++pending_row_count_;
  return ret;
}

int ObFileTableAccessOp::build_scan_options(ObFileScanOptions &options)
{
  int ret = OB_SUCCESS;
  options.path_ = MY_SPEC.path_;
  options.format_ = static_cast<ObFileScanFormat>(MY_SPEC.format_);
  options.header_ = MY_SPEC.header_;
  options.has_header_ = true;
  options.sample_size_ = -1;
  options.null_str_ = MY_SPEC.null_str_;
  options.delimiter_ = MY_SPEC.delimiter_;
  options.quote_ = MY_SPEC.quote_;
  options.escape_ = MY_SPEC.escape_;
  if (options.delimiter_.empty()) {
    options.delimiter_ = ",";
  }
  if (options.quote_.empty()) {
    options.quote_ = "\"";
  }
  return ret;
}

int ObFileTableAccessOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObFileScanOptions options;
  file_index_ = 0;
  row_group_index_ = 0;
  row_index_in_group_ = 0;
  row_count_in_group_ = 0;
  pending_row_index_ = 0;
  pending_row_count_ = 0;
  pending_values_.clear();
  pending_strings_.clear();
  pending_row_ends_.clear();
  is_eof_ = false;
  is_opened_ = true;
  if (is_file_opened_) {
    file_reader_.close();
    is_file_opened_ = false;
  }
  file_offset_ = 0;
  file_size_ = 0;
  is_file_end_ = false;
  if (OB_FAIL(build_scan_options(options))) {
  } else if (MY_SPEC.output_.empty()) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObFileScanFormat::GLOB == options.format_) {
    // the glob function returns the list of the files
  } else if (OB_FAIL(open_next_file())) {
  } else if (ObFileScanFormat::CSV == options.format_) {
    ret = csv_reader_.init(options, options.header_, MY_SPEC.column_exprs_.count());
  } else if (ObFileScanFormat::JSONL == options.format_) {
    std::vector<common::ObString> names;
    for (int64_t i = 0; i < MY_SPEC.column_names_.count(); ++i) {
      names.push_back(MY_SPEC.column_names_.at(i));
    }
    ret = json_reader_.init(options, &names);
  }
  return ret;
}

int ObFileTableAccessOp::open_next_file()
{
  int ret = OB_SUCCESS;
  ObFileScanOptions options;
  if (OB_FAIL(build_scan_options(options))) {
  } else if (OB_ISNULL(file_source_)) {
    void *buf = NULL;
    if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObFileTableFileSource)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      file_source_ = new (buf) ObFileTableFileSource();
      if (OB_FAIL(file_source_->open(options.path_))) {
        ret = OB_FILE_NOT_EXIST;
        LOG_USER_ERROR(OB_FILE_NOT_EXIST, options.path_.ptr());
      }
    }
  }
  if (OB_SUCC(ret) && ObFileScanFormat::PARQUET == options.format_) {
    ret = parquet_reader_.init(file_source_);
  }
  return ret;
}

int ObFileTableAccessOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else {
    file_index_ = 0;
    row_group_index_ = 0;
    row_index_in_group_ = 0;
    row_count_in_group_ = 0;
    pending_row_index_ = 0;
    pending_row_count_ = 0;
    pending_values_.clear();
    pending_strings_.clear();
    pending_row_ends_.clear();
    parquet_group_columns_.clear();
    is_eof_ = false;
    if (is_file_opened_) {
      file_reader_.close();
      is_file_opened_ = false;
    }
    file_offset_ = 0;
    file_size_ = 0;
    is_file_end_ = false;
  }
  return ret;
}

int ObFileTableAccessOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_switch_iterator())) {
  } else {
    file_index_ = 0;
    row_group_index_ = 0;
    row_index_in_group_ = 0;
    row_count_in_group_ = 0;
    pending_row_index_ = 0;
    pending_row_count_ = 0;
    pending_values_.clear();
    pending_strings_.clear();
    pending_row_ends_.clear();
    parquet_group_columns_.clear();
    is_eof_ = false;
    if (is_file_opened_) {
      file_reader_.close();
      is_file_opened_ = false;
    }
    file_offset_ = 0;
    file_size_ = 0;
    is_file_end_ = false;
  }
  return ret;
}

int ObFileTableAccessOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_check_status())) {
  } else {
    clear_evaluated_flag();
    if (OB_FAIL(read_next_record())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to read the next record of the file", K(ret));
      }
    }
  }
  return ret;
}

int ObFileTableAccessOp::read_next_glob_record()
{
  int ret = OB_SUCCESS;
  const int64_t count = MY_SPEC.file_list_.count();
  if (file_index_ >= count) {
    ret = OB_ITER_END;
  } else {
    const common::ObString &file = MY_SPEC.file_list_.at(file_index_);
    ++file_index_;
    ObExpr *expr = MY_SPEC.column_exprs_.at(0);
    ObDatum *datum = &expr->locate_datum_for_write(eval_ctx_);
    char *buffer = expr->get_str_res_mem(eval_ctx_, file.length());
    if (NULL == buffer) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      memcpy(buffer, file.ptr(), file.length());
      datum->set_string(buffer, file.length());
      expr->set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

int ObFileTableAccessOp::read_next_parquet_record()
{
  int ret = OB_SUCCESS;
  const std::vector<ObParquetRowGroupMeta> &row_groups = parquet_reader_.get_row_groups();
  const std::vector<ObParquetColumnMeta> &columns = parquet_reader_.get_columns();
  while (OB_SUCC(ret)) {
    if (row_group_index_ >= static_cast<int64_t>(row_groups.size())) {
      ret = OB_ITER_END;
      break;
    }
    if (parquet_group_columns_.empty()) {
      // the values of a row group are read when the group is entered
      const ObParquetRowGroupMeta &row_group = row_groups[row_group_index_];
      row_count_in_group_ = row_group.num_rows_;
      row_index_in_group_ = 0;
      for (size_t i = 0; OB_SUCC(ret) && i < columns.size(); ++i) {
        std::vector<ObParquetValue> values;
        if (OB_FAIL(parquet_reader_.read_column(row_group_index_, (int64_t)i, values))) {
          break;
        }
        parquet_group_columns_.push_back(values);
      }
    }
    if (row_index_in_group_ >= row_count_in_group_) {
      // the group is exhausted, the next one is read
      ++row_group_index_;
      row_index_in_group_ = 0;
      row_count_in_group_ = 0;
      parquet_group_columns_.clear();
      continue;
    }
    for (size_t i = 0; OB_SUCC(ret) && i < parquet_group_columns_.size(); ++i) {
      if (static_cast<int64_t>(i) >= row_count_in_group_
          || static_cast<int64_t>(i) >= static_cast<int64_t>(parquet_group_columns_[i].size())) {
        ret = set_null_datum((int64_t)i);
      } else {
        ret = set_row_datum((int64_t)i, parquet_group_columns_[i][row_index_in_group_]);
      }
    }
    ++row_index_in_group_;
    break;
  }
  return ret;
}

int ObFileTableAccessOp::consume_pending_row()
{
  int ret = OB_SUCCESS;
  const int64_t begin = (pending_row_index_ == 0) ? 0 : pending_row_ends_[pending_row_index_ - 1];
  const int64_t end = pending_row_ends_[pending_row_index_];
  if (end - begin != MY_SPEC.column_exprs_.count()) {
    // the file has changed since the statement was resolved
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "the number of the columns of the file has changed");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < end - begin; ++i) {
      // the strings of the record are copied, the pointers are set again
      ObFileScanValue &value = pending_values_[begin + i];
      if (!value.is_null_ && !pending_strings_[begin + i].empty()) {
        value.value_.assign_ptr(pending_strings_[begin + i].c_str(),
                                pending_strings_[begin + i].length());
      }
      ret = set_row_datum(i, value.value_, value.is_null_);
    }
  }
  ++pending_row_index_;
  if (pending_row_index_ >= pending_row_count_) {
    pending_values_.clear();
    pending_strings_.clear();
    pending_row_ends_.clear();
    pending_row_index_ = 0;
    pending_row_count_ = 0;
  }
  return ret;
}

int ObFileTableAccessOp::read_file_chunk(ObFileScanReader &reader)
{
  int ret = OB_SUCCESS;
  ObFileScanOptions options;
  char *buffer = NULL;
  if (OB_FAIL(build_scan_options(options))) {
  } else if (!is_file_opened_) {
    if (OB_FAIL(file_reader_.open(options.path_, false))) {
      ret = OB_FILE_NOT_EXIST;
      LOG_USER_ERROR(OB_FILE_NOT_EXIST, options.path_.ptr());
    } else {
      file_size_ = common::get_file_size(options.path_.ptr());
      file_offset_ = 0;
      is_file_end_ = (0 == file_size_);
      is_file_opened_ = true;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(buffer = static_cast<char *>(allocator_->alloc(FILE_TABLE_READ_CHUNK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  while (OB_SUCC(ret) && !is_file_end_) {
    int64_t read_size = 0;
    const int64_t want = (file_size_ - file_offset_) < FILE_TABLE_READ_CHUNK_SIZE
                             ? (file_size_ - file_offset_)
                             : FILE_TABLE_READ_CHUNK_SIZE;
    if (OB_FAIL(file_reader_.pread(buffer, want, file_offset_, read_size))) {
      ret = OB_IO_ERROR;
      break;
    }
    if (read_size <= 0) {
      is_file_end_ = true;
      break;
    }
    file_offset_ += read_size;
    is_file_end_ = (file_offset_ >= file_size_);
    if (OB_FAIL(reader.feed(buffer, read_size, is_file_end_, *this))) {
      break;
    }
    if (pending_row_count_ > 0) {
      // the records of the chunk are enough for the next rows
      break;
    }
  }
  return ret;
}

int ObFileTableAccessOp::read_next_record()
{
  int ret = OB_SUCCESS;
  ObFileScanOptions options;
  if (OB_FAIL(build_scan_options(options))) {
  } else if (ObFileScanFormat::GLOB == options.format_) {
    ret = read_next_glob_record();
  } else if (ObFileScanFormat::PARQUET == options.format_) {
    ret = read_next_parquet_record();
  } else if (pending_row_index_ < pending_row_count_) {
    // a record that was read before is still waiting
    ret = consume_pending_row();
  } else {
    ObFileScanReader *reader = NULL;
    if (ObFileScanFormat::CSV == options.format_) {
      reader = &csv_reader_;
    } else {
      reader = &json_reader_;
    }
    if (OB_FAIL(read_file_chunk(*reader))) {
    } else if (pending_row_count_ <= 0) {
      ret = OB_ITER_END;
    } else {
      ret = consume_pending_row();
    }
  }
  return ret;
}

int ObFileTableAccessOp::set_null_datum(int64_t index)
{
  int ret = OB_SUCCESS;
  if (index >= MY_SPEC.column_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObExpr *expr = MY_SPEC.column_exprs_.at(index);
    ObDatum *datum = &expr->locate_datum_for_write(eval_ctx_);
    datum->set_null();
    expr->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}

int ObFileTableAccessOp::set_row_datum(int64_t index, const common::ObString &value, bool is_null)
{
  int ret = OB_SUCCESS;
  if (index >= MY_SPEC.column_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObExpr *expr = MY_SPEC.column_exprs_.at(index);
    ObDatum *datum = &expr->locate_datum_for_write(eval_ctx_);
    if (is_null) {
      datum->set_null();
    } else {
      const common::ObObjType type = expr->datum_meta_.type_;
      switch (type) {
      case common::ObIntType:
      case common::ObTinyIntType:
      case common::ObMediumIntType:
      case common::ObSmallIntType: {
        int64_t int_value = 0;
        if (ob_file_scan_is_bool_text(value.ptr(), value.length())) {
          int_value = (0 == strncasecmp(value.ptr(), "true", 4)) ? 1 : 0;
          datum->set_int(int_value);
        } else if (!ob_file_scan_try_parse_int64(value.ptr(), value.length(), int_value)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_USER_ERROR(OB_ERR_UNEXPECTED, "the value of the file is not a number");
        } else {
          datum->set_int(int_value);
        }
        break;
      }
      case common::ObDoubleType:
      case common::ObFloatType: {
        double double_value = 0.0;
        if (!ob_file_scan_try_parse_double(value.ptr(), value.length(), double_value)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_USER_ERROR(OB_ERR_UNEXPECTED, "the value of the file is not a number");
        } else {
          datum->set_double(double_value);
        }
        break;
      }
      case common::ObDateType: {
        int32_t date_value = 0;
        if (OB_FAIL(ObTimeConverter::str_to_date(value, date_value, 0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_USER_ERROR(OB_ERR_UNEXPECTED, "the value of the file is not a date");
        } else {
          datum->set_date(date_value);
        }
        break;
      }
      case common::ObDateTimeType: {
        std::string normalized;
        ob_file_scan_normalize_datetime(value, normalized);
        const common::ObString datetime_value(normalized.length(), normalized.data());
        int64_t datetime_int_value = 0;
        int16_t scale = 0;
        ObTimeConvertCtx convert_ctx(GET_MY_SESSION(ctx_)->get_timezone_info(), false);
        if (OB_FAIL(ObTimeConverter::str_to_datetime(datetime_value,
                                                    convert_ctx,
                                                    datetime_int_value,
                                                    &scale,
                                                    0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_USER_ERROR(OB_ERR_UNEXPECTED, "the value of the file is not a date time");
        } else {
          datum->set_datetime(datetime_int_value);
        }
        break;
      }
      case common::ObVarcharType:
      case common::ObCharType:
      default: {
        char *buffer = expr->get_str_res_mem(eval_ctx_, value.length());
        if (NULL == buffer) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          memcpy(buffer, value.ptr(), value.length());
          datum->set_string(buffer, value.length());
        }
        break;
      }
      }
    }
    if (OB_SUCC(ret)) {
      expr->set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

int ObFileTableAccessOp::set_row_datum(int64_t index, const ObParquetValue &value)
{
  int ret = OB_SUCCESS;
  if (index >= MY_SPEC.column_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObExpr *expr = MY_SPEC.column_exprs_.at(index);
    ObDatum *datum = &expr->locate_datum_for_write(eval_ctx_);
    if (value.is_null_) {
      datum->set_null();
    } else {
      const common::ObObjType type = expr->datum_meta_.type_;
      switch (type) {
      case common::ObIntType:
      case common::ObTinyIntType:
      case common::ObMediumIntType:
      case common::ObSmallIntType:
        datum->set_int(value.int_value_);
        break;
      case common::ObDoubleType:
      case common::ObFloatType:
        datum->set_double(value.double_value_);
        break;
      case common::ObVarcharType:
      case common::ObCharType:
      default: {
        const int64_t length = value.bytes_len_;
        char *buffer = expr->get_str_res_mem(eval_ctx_, length);
        if (NULL == buffer) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          if (length > 0) {
            memcpy(buffer, value.bytes_ptr_, length);
          }
          datum->set_string(buffer, length);
        }
        break;
      }
      }
    }
    if (OB_SUCC(ret)) {
      expr->set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

int ObFileTableAccessOp::inner_close()
{
  int ret = OB_SUCCESS;
  file_index_ = 0;
  row_group_index_ = 0;
  row_index_in_group_ = 0;
  row_count_in_group_ = 0;
  pending_row_index_ = 0;
  pending_row_count_ = 0;
  pending_values_.clear();
  pending_strings_.clear();
  pending_row_ends_.clear();
  parquet_group_columns_.clear();
  is_eof_ = false;
  is_opened_ = false;
  return ret;
}

} // namespace sql
} // namespace oceanbase
