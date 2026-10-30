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

#include "sql/engine/basic/ob_file_scan_utils.h"

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace oceanbase
{
namespace sql
{

const char *ob_file_scan_type_name(ObFileScanType type)
{
  const char *name = "NULL";
  switch (type) {
  case ObFileScanType::BIGINT:
    name = "BIGINT";
    break;
  case ObFileScanType::DOUBLE:
    name = "DOUBLE";
    break;
  case ObFileScanType::BOOLEAN:
    name = "BOOLEAN";
    break;
  case ObFileScanType::VARCHAR:
    name = "VARCHAR";
    break;
  case ObFileScanType::DATE:
    name = "DATE";
    break;
  case ObFileScanType::DATETIME:
    name = "DATETIME";
    break;
  case ObFileScanType::UNKNOWN:
  default:
    name = "NULL";
    break;
  }
  return name;
}

bool ob_file_scan_is_valid_date(int64_t year, int64_t month, int64_t day)
{
  static const int days_of_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (year < 1000 || year > 9999 || month < 1 || month > 12 || day < 1) {
    return false;
  }
  int64_t limit = days_of_month[month - 1];
  if (2 == month && ((0 == year % 4 && 0 != year % 100) || 0 == year % 400)) {
    limit = 29;
  }
  return day <= limit;
}

bool ob_file_scan_try_parse_int64(const char *data, int64_t length, int64_t &value)
{
  bool ret = false;
  if (NULL == data || length <= 0) {
    // not a number
  } else {
    int64_t pos = 0;
    bool negative = false;
    if ('+' == data[0] || '-' == data[0]) {
      negative = ('-' == data[0]);
      pos = 1;
    }
    if (pos < length) {
      uint64_t acc = 0;
      bool overflow = false;
      for (; pos < length; ++pos) {
        if (!isdigit(static_cast<unsigned char>(data[pos]))) {
          break;
        }
        const uint64_t digit = static_cast<uint64_t>(data[pos] - '0');
        if (acc > (UINT64_MAX - digit) / 10) {
          overflow = true;
          break;
        }
        acc = acc * 10 + digit;
      }
      if (!overflow && pos == length) {
        if (negative) {
          if (acc <= static_cast<uint64_t>(INT64_MAX) + 1) {
            value = (acc == static_cast<uint64_t>(INT64_MAX) + 1) ? INT64_MIN
                                                                   : -static_cast<int64_t>(acc);
            ret = true;
          }
        } else if (acc <= static_cast<uint64_t>(INT64_MAX)) {
          value = static_cast<int64_t>(acc);
          ret = true;
        }
      }
    }
  }
  return ret;
}

bool ob_file_scan_try_parse_double(const char *data, int64_t length, double &value)
{
  bool ret = false;
  if (NULL == data || length <= 0) {
    // not a number
  } else {
    bool has_digit = false;
    bool has_dot = false;
    bool has_exp = false;
    int64_t pos = 0;
    if ('+' == data[0] || '-' == data[0]) {
      pos = 1;
    }
    int64_t scan = pos;
    for (; scan < length; ++scan) {
      const char c = data[scan];
      if (isdigit(static_cast<unsigned char>(c))) {
        has_digit = true;
      } else if ('.' == c && !has_dot && !has_exp) {
        has_dot = true;
      } else if (('e' == c || 'E' == c) && has_digit && !has_exp) {
        has_exp = true;
      } else {
        break;
      }
    }
    if (has_digit && scan == length) {
      char buf[64];
      const int64_t copy_len = (length < static_cast<int64_t>(sizeof(buf)) - 1)
                                   ? length
                                   : static_cast<int64_t>(sizeof(buf)) - 1;
      memcpy(buf, data, copy_len);
      buf[copy_len] = '\0';
      char *endptr = NULL;
      const double d = strtod(buf, &endptr);
      if (NULL != endptr && '\0' == *endptr) {
        value = d;
        ret = true;
      }
    }
  }
  return ret;
}

bool ob_file_scan_is_bool_text(const char *data, int64_t length)
{
  bool ret = false;
  if (NULL != data && length > 0) {
    if (4 == length && 0 == strncasecmp(data, "true", 4)) {
      ret = true;
    } else if (5 == length && 0 == strncasecmp(data, "false", 5)) {
      ret = true;
    }
  }
  return ret;
}

namespace
{
inline bool is_digit_char(char c) { return c >= '0' && c <= '9'; }

/// Parse the ``YYYY-MM-DD`` prefix of a date like text.
bool parse_date_prefix(const char *data, int64_t length, int64_t &year, int64_t &month, int64_t &day)
{
  if (length < 10 || !is_digit_char(data[0]) || !is_digit_char(data[1]) || !is_digit_char(data[2])
      || !is_digit_char(data[3]) || '-' != data[4] || !is_digit_char(data[5]) || !is_digit_char(data[6])
      || '-' != data[7] || !is_digit_char(data[8]) || !is_digit_char(data[9])) {
    return false;
  }
  year = (data[0] - '0') * 1000 + (data[1] - '0') * 100 + (data[2] - '0') * 10 + (data[3] - '0');
  month = (data[5] - '0') * 10 + (data[6] - '0');
  day = (data[8] - '0') * 10 + (data[9] - '0');
  return true;
}
} // namespace

bool ob_file_scan_is_date_text(const char *data, int64_t length)
{
  bool ret = false;
  int64_t year = 0;
  int64_t month = 0;
  int64_t day = 0;
  if (parse_date_prefix(data, length, year, month, day) && 10 == length) {
    ret = ob_file_scan_is_valid_date(year, month, day);
  }
  return ret;
}

bool ob_file_scan_is_datetime_text(const char *data, int64_t length)
{
  bool ret = false;
  int64_t year = 0;
  int64_t month = 0;
  int64_t day = 0;
  if (parse_date_prefix(data, length, year, month, day)) {
    int64_t pos = 10;
    if (pos == length) {
      // a bare date is not a date time
    } else if (' ' == data[pos] || 'T' == data[pos] || 't' == data[pos]) {
      ++pos;
      if (pos + 8 <= length && is_digit_char(data[pos]) && is_digit_char(data[pos + 1])
          && ':' == data[pos + 2] && is_digit_char(data[pos + 3]) && is_digit_char(data[pos + 4])
          && ':' == data[pos + 5] && is_digit_char(data[pos + 6]) && is_digit_char(data[pos + 7])) {
        const int64_t hour = (data[pos] - '0') * 10 + (data[pos + 1] - '0');
        const int64_t minute = (data[pos + 3] - '0') * 10 + (data[pos + 4] - '0');
        const int64_t second = (data[pos + 6] - '0') * 10 + (data[pos + 7] - '0');
        if (hour < 24 && minute < 60 && second < 60) {
          int64_t end = pos + 8;
          if (end < length && '.' == data[end]) {
            ++end;
            const int64_t frac_begin = end;
            while (end < length && is_digit_char(data[end])) {
              ++end;
            }
            if (end == frac_begin) {
              return false;
            }
          }
          ret = (end == length);
        }
      }
    }
  }
  return ret;
}

void ob_file_scan_normalize_datetime(const common::ObString &text, std::string &out)
{
  out.clear();
  const char *data = text.ptr();
  const int64_t length = text.length();
  if (NULL == data || length <= 0) {
    return;
  }
  int64_t year = 0;
  int64_t month = 0;
  int64_t day = 0;
  if (!parse_date_prefix(data, length, year, month, day)) {
    out.append(data, length);
    return;
  }
  char head[32];
  snprintf(head, sizeof(head), "%04lld-%02lld-%02lld", static_cast<long long>(year),
           static_cast<long long>(month), static_cast<long long>(day));
  out.append(head);
  if (length <= 10) {
    out.append(" 00:00:00");
    return;
  }
  int64_t pos = 10;
  if (' ' == data[pos] || 'T' == data[pos] || 't' == data[pos]) {
    ++pos;
  }
  out.append(" ");
  out.append(data + pos, length - pos);
}

ObFileScanType ob_file_scan_merge_type(ObFileScanType lhs, ObFileScanType rhs)
{
  ObFileScanType result = lhs;
  if (ObFileScanType::UNKNOWN == lhs) {
    result = rhs;
  } else if (ObFileScanType::UNKNOWN == rhs) {
    result = lhs;
  } else if (lhs == rhs) {
    result = lhs;
  } else if ((ObFileScanType::BIGINT == lhs && ObFileScanType::DOUBLE == rhs)
             || (ObFileScanType::DOUBLE == lhs && ObFileScanType::BIGINT == rhs)) {
    result = ObFileScanType::DOUBLE;
  } else if ((ObFileScanType::DATE == lhs && ObFileScanType::DATETIME == rhs)
             || (ObFileScanType::DATETIME == lhs && ObFileScanType::DATE == rhs)) {
    result = ObFileScanType::DATETIME;
  } else {
    result = ObFileScanType::VARCHAR;
  }
  return result;
}

void ObFileTypeInferer::merge_type(ObFileScanType other)
{
  type_ = ob_file_scan_merge_type(type_, other);
  has_value_ = has_value_ || (ObFileScanType::UNKNOWN != other);
}

void ObFileTypeInferer::observe_text(const common::ObString &text)
{
  if (text.empty()) {
    observe_null();
    return;
  }
  const char *data = text.ptr();
  const int64_t length = text.length();
  ObFileScanType candidate = ObFileScanType::VARCHAR;
  if (ob_file_scan_is_bool_text(data, length)) {
    candidate = ObFileScanType::BOOLEAN;
  } else if (ob_file_scan_is_datetime_text(data, length)) {
    candidate = ObFileScanType::DATETIME;
  } else if (ob_file_scan_is_date_text(data, length)) {
    candidate = ObFileScanType::DATE;
  } else {
    int64_t int_value = 0;
    double dbl_value = 0.0;
    if (ob_file_scan_try_parse_int64(data, length, int_value)) {
      candidate = ObFileScanType::BIGINT;
    } else if (ob_file_scan_try_parse_double(data, length, dbl_value)) {
      candidate = ObFileScanType::DOUBLE;
    }
  }
  merge_type(candidate);
}

void ObFileTypeInferer::observe_json_string(const common::ObString &text)
{
  if (text.empty()) {
    observe_null();
    return;
  }
  const char *data = text.ptr();
  const int64_t length = text.length();
  ObFileScanType candidate = ObFileScanType::VARCHAR;
  if (ob_file_scan_is_datetime_text(data, length)) {
    candidate = ObFileScanType::DATETIME;
  } else if (ob_file_scan_is_date_text(data, length)) {
    candidate = ObFileScanType::DATE;
  }
  merge_type(candidate);
}

void ObFileTypeInferer::observe_json_int() { merge_type(ObFileScanType::BIGINT); }

void ObFileTypeInferer::observe_json_double() { merge_type(ObFileScanType::DOUBLE); }

void ObFileTypeInferer::observe_json_bool() { merge_type(ObFileScanType::BOOLEAN); }

void ObFileTypeInferer::merge(const ObFileTypeInferer &other)
{
  if (other.nullable_) {
    nullable_ = true;
  }
  if (other.has_value_) {
    merge_type(other.type_);
  }
}

/* ------------------------------------------------------------------------- */
/* Schema inferer                                                            */
/* ------------------------------------------------------------------------- */

int ObFileSchemaInferer::on_row(const ObFileScanValue *values, int64_t count)
{
  int ret = OB_SUCCESS;
  if (count < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (inferers_.size() < static_cast<size_t>(count)) {
    inferers_.resize(count);
    present_counts_.resize(count, 0);
  }
  for (int64_t i = 0; i < count; ++i) {
    ++present_counts_[i];
    if (values[i].is_null_) {
      inferers_[i].observe_null();
      continue;
    }
    switch (values[i].kind_) {
    case ObFileScanValueKind::JSON_STRING:
      inferers_[i].observe_json_string(values[i].value_);
      break;
    case ObFileScanValueKind::JSON_INT:
      inferers_[i].observe_json_int();
      break;
    case ObFileScanValueKind::JSON_DOUBLE:
      inferers_[i].observe_json_double();
      break;
    case ObFileScanValueKind::JSON_BOOL:
      inferers_[i].observe_json_bool();
      break;
    case ObFileScanValueKind::TEXT:
    default:
      inferers_[i].observe_text(values[i].value_);
      break;
    }
  }
  ++row_count_;
  return ret;
}

int ObFileSchemaInferer::build_columns(const std::vector<common::ObString> &names,
                                        std::vector<ObFileScanColumn> &columns) const
{
  int ret = OB_SUCCESS;
  columns.clear();
  for (size_t i = 0; i < inferers_.size(); ++i) {
    ObFileScanColumn column;
    column.name_ = (i < names.size()) ? names[i] : common::ObString();
    column.type_ = inferers_[i].get_type();
    column.nullable_ = inferers_[i].is_nullable() || (present_counts_[i] < row_count_);
    columns.push_back(column);
  }
  return ret;
}

/* ------------------------------------------------------------------------- */
/* CSV reader                                                                */
/* ------------------------------------------------------------------------- */

ObFileCsvReader::ObFileCsvReader()
    : skip_header_(false),
      expected_field_count_(0),
      field_count_(0),
      header_seen_(false),
      header_record_(false),
      record_count_(0),
      line_number_(1),
      head_(0),
      tail_(0)
{}

ObFileCsvReader::~ObFileCsvReader() {}

int ObFileCsvReader::init(const ObFileScanOptions &options, bool skip_header, int64_t expected_field_count)
{
  int ret = OB_SUCCESS;
  options_ = options;
  skip_header_ = skip_header;
  expected_field_count_ = expected_field_count;
  field_count_ = expected_field_count;
  header_seen_ = false;
  header_record_ = skip_header;
  record_count_ = 0;
  line_number_ = 1;
  head_ = 0;
  tail_ = 0;
  buf_.clear();
  scratch_.clear();
  values_.clear();
  header_.clear();
  header_storage_.clear();
  field_raw_.clear();
  field_off_.clear();
  field_len_.clear();
  if (options_.max_field_size_ <= 0 || options_.max_record_size_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObFileCsvReader::check_field_count(int64_t count)
{
  int ret = OB_SUCCESS;
  if (field_count_ <= 0) {
    field_count_ = count;
  } else if (count != field_count_) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObFileCsvReader::emit_record(ObFileScanSink &sink)
{
  int ret = OB_SUCCESS;
  // materialize the field values, the scratch buffer may have been reallocated
  values_.clear();
  for (size_t i = 0; i < field_off_.size(); ++i) {
    common::ObString value;
    if (field_raw_[i]) {
      value.assign_ptr(field_raw_ptr_[i], field_len_[i]);
    } else {
      value.assign_ptr(scratch_.data() + field_off_[i], field_len_[i]);
    }
    values_.push_back(ObFileScanValue(value, 0 != null_flags_[i]));
  }
  if (header_record_) {
    header_record_ = false;
    header_seen_ = true;
    header_.clear();
    header_storage_.clear();
    std::vector<int64_t> header_off;
    std::vector<int32_t> header_len;
    header_off.reserve(values_.size());
    header_len.reserve(values_.size());
    for (size_t i = 0; i < values_.size(); ++i) {
      header_off.push_back(static_cast<int64_t>(header_storage_.size()));
      header_len.push_back(values_[i].value_.length());
      header_storage_.append(values_[i].value_.ptr(), values_[i].value_.length());
    }
    for (size_t i = 0; i < values_.size(); ++i) {
      header_.push_back(common::ObString(header_len[i], header_storage_.data() + header_off[i]));
    }
    if (expected_field_count_ > 0) {
      if (OB_FAIL(check_field_count(values_.size()))) {
        return ret;
      }
    } else {
      field_count_ = values_.size();
    }
  } else {
    if (OB_FAIL(check_field_count(values_.size()))) {
      return ret;
    } else if (OB_FAIL(sink.on_row(values_.data(), values_.size()))) {
      return ret;
    } else {
      ++record_count_;
    }
  }
  return ret;
}

int ObFileCsvReader::parse_field(int64_t &cursor, bool is_end, bool &field_complete)
{
  int ret = OB_SUCCESS;
  field_complete = false;
  const char delimiter = options_.delimiter_.empty() ? ',' : options_.delimiter_.ptr()[0];
  const char quote = options_.quote_.empty() ? '\0' : options_.quote_.ptr()[0];
  const char escape = options_.escape_.empty() ? '\0' : options_.escape_.ptr()[0];
  const char *field_ptr = NULL;
  int32_t field_len = 0;
  bool is_raw = true;
  bool is_null = false;
  int64_t field_off = 0;

  if (cursor < tail_ && '\0' != quote && quote == buf_[cursor]) {
    // a quoted field
    field_off = static_cast<int64_t>(scratch_.size());
    ++cursor;
    int64_t run_start = cursor;
    bool closed = false;
    bool need_more = false;
    while (cursor < tail_) {
      const char c = buf_[cursor];
      if ('\0' != escape && escape == c) {
        if (cursor + 1 >= tail_) {
          need_more = true;
          break;
        }
        scratch_.append(&buf_[run_start], cursor - run_start);
        scratch_.push_back(buf_[cursor + 1]);
        cursor += 2;
        run_start = cursor;
        continue;
      }
      if ('\0' != quote && quote == c) {
        if (cursor + 1 < tail_) {
          if (quote == buf_[cursor + 1]) {
            scratch_.append(&buf_[run_start], cursor - run_start);
            scratch_.push_back(quote);
            cursor += 2;
            run_start = cursor;
            continue;
          }
        } else if (!is_end) {
          need_more = true;
          break;
        }
        scratch_.append(&buf_[run_start], cursor - run_start);
        ++cursor;
        closed = true;
        break;
      }
      ++cursor;
    }
    if (need_more) {
      field_complete = false;
      return OB_SUCCESS;
    }
    if (!closed) {
      if (!is_end) {
        field_complete = false;
        return OB_SUCCESS;
      }
      ret = OB_ERR_UNEXPECTED;
    } else {
      is_raw = false;
      field_len = static_cast<int32_t>(scratch_.size() - field_off);
    }
  } else {
    // an unquoted field
    const int64_t start = cursor;
    while (cursor < tail_) {
      const char c = buf_[cursor];
      if (c == delimiter || '\n' == c || '\r' == c) {
        break;
      }
      ++cursor;
    }
    if (cursor >= tail_ && !is_end) {
      field_complete = false;
      return OB_SUCCESS;
    }
    field_ptr = &buf_[start];
    field_len = static_cast<int32_t>(cursor - start);
    if (0 == field_len) {
      is_null = true;
    } else if (!options_.null_str_.empty() && options_.null_str_.length() == field_len
               && 0 == memcmp(field_ptr, options_.null_str_.ptr(), field_len)) {
      is_null = true;
    }
  }

  if (OB_SUCC(ret)) {
    field_raw_.push_back(is_raw ? 1 : 0);
    field_raw_ptr_.push_back(field_ptr);
    field_off_.push_back(field_off);
    field_len_.push_back(field_len);
    null_flags_.push_back(is_null ? 1 : 0);
    field_complete = true;
  }
  return ret;
}

int ObFileCsvReader::parse_record(int64_t &cursor, bool is_end, bool &record_complete)
{
  int ret = OB_SUCCESS;
  record_complete = false;
  field_raw_.clear();
  field_raw_ptr_.clear();
  field_off_.clear();
  field_len_.clear();
  null_flags_.clear();
  scratch_.clear();
  const char delimiter = options_.delimiter_.empty() ? ',' : options_.delimiter_.ptr()[0];

  while (true) {
    if (cursor >= tail_) {
      break;
    }
    bool field_complete = false;
    if (OB_FAIL(parse_field(cursor, is_end, field_complete))) {
      return ret;
    }
    if (!field_complete) {
      // the record is not complete yet
      return OB_SUCCESS;
    }
    if (cursor < tail_) {
      const char c = buf_[cursor];
      if (delimiter == c) {
        ++cursor;
        continue;
      }
      if ('\r' == c) {
        ++cursor;
        if (cursor < tail_) {
          if ('\n' == buf_[cursor]) {
            ++cursor;
          }
        } else if (!is_end) {
          // the record terminator is not complete yet
          return OB_SUCCESS;
        }
        ++line_number_;
        record_complete = true;
        break;
      }
      if ('\n' == c) {
        ++cursor;
        ++line_number_;
        record_complete = true;
        break;
      }
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    if (!is_end) {
      // the record terminator is not complete yet
      return OB_SUCCESS;
    }
    // the file ends without a trailing line terminator
    record_complete = true;
    break;
  }
  return ret;
}

int ObFileCsvReader::scan_records(bool is_end, ObFileScanSink &sink)
{
  int ret = OB_SUCCESS;
  while (true) {
    // skip the blank lines
    while (head_ < tail_ && ('\n' == buf_[head_] || '\r' == buf_[head_])) {
      if ('\n' == buf_[head_]) {
        ++line_number_;
      }
      ++head_;
    }
    if (head_ >= tail_) {
      break;
    }
    int64_t cursor = head_;
    bool record_complete = false;
    if (OB_FAIL(parse_record(cursor, is_end, record_complete))) {
      return ret;
    }
    if (!record_complete) {
      break;
    }
    head_ = cursor;
    if (OB_FAIL(emit_record(sink))) {
      return ret;
    }
  }
  return ret;
}

int ObFileCsvReader::feed(const char *data, int64_t length, bool is_end, ObFileScanSink &sink)
{
  int ret = OB_SUCCESS;
  if (length > 0) {
    if (NULL == data) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      // drop the consumed bytes and append the new chunk
      if (head_ > 0) {
        if (tail_ > head_) {
          memmove(&buf_[0], &buf_[head_], tail_ - head_);
        }
        tail_ -= head_;
        head_ = 0;
      }
      if (tail_ + length > static_cast<int64_t>(buf_.size())) {
        buf_.resize(tail_ + length);
      }
      memcpy(&buf_[tail_], data, length);
      tail_ += length;
      // strip the UTF-8 byte order mark of the very first chunk
      if (0 == record_count_ && !header_seen_ && tail_ >= 3 && 0xEF == (unsigned char)buf_[0]
          && 0xBB == (unsigned char)buf_[1] && 0xBF == (unsigned char)buf_[2]) {
        memmove(&buf_[0], &buf_[3], tail_ - 3);
        tail_ -= 3;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scan_records(is_end, sink))) {
    } else if (is_end) {
      head_ = tail_;
    }
  }
  return ret;
}

/* ------------------------------------------------------------------------- */
/* JSON reader                                                               */
/* ------------------------------------------------------------------------- */

ObFileJsonReader::ObFileJsonReader() : collect_names_(true), record_count_(0), head_(0), tail_(0) {}

ObFileJsonReader::~ObFileJsonReader() {}

int ObFileJsonReader::init(const ObFileScanOptions &options,
                           const std::vector<common::ObString> *expected_names)
{
  int ret = OB_SUCCESS;
  options_ = options;
  record_count_ = 0;
  head_ = 0;
  tail_ = 0;
  buf_.clear();
  scratch_.clear();
  values_.clear();
  field_names_.clear();
  expected_names_.clear();
  name_pool_.clear();
  collect_names_ = (NULL == expected_names || expected_names->empty());
  if (!collect_names_) {
    expected_names_.assign(expected_names->begin(), expected_names->end());
    field_names_ = expected_names_;
  }
  return ret;
}

int ObFileJsonReader::skip_ws(int64_t &pos, int64_t end)
{
  while (pos < end && (' ' == buf_[pos] || '\t' == buf_[pos] || '\r' == buf_[pos] || '\n' == buf_[pos])) {
    ++pos;
  }
  return OB_SUCCESS;
}

int ObFileJsonReader::parse_string(int64_t &pos, int64_t end, std::string &out)
{
  int ret = OB_SUCCESS;
  out.clear();
  if (pos >= end || '"' != buf_[pos]) {
    ret = OB_ERR_UNEXPECTED;
    return ret;
  }
  ++pos;
  while (pos < end) {
    const char c = buf_[pos];
    if ('\\' == c) {
      ++pos;
      if (pos >= end) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      const char esc = buf_[pos];
      switch (esc) {
      case '"':
        out.push_back('"');
        break;
      case '\\':
        out.push_back('\\');
        break;
      case '/':
        out.push_back('/');
        break;
      case 'b':
        out.push_back('\b');
        break;
      case 'f':
        out.push_back('\f');
        break;
      case 'n':
        out.push_back('\n');
        break;
      case 'r':
        out.push_back('\r');
        break;
      case 't':
        out.push_back('\t');
        break;
      case 'u': {
        if (pos + 4 >= end) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        int code = 0;
        for (int i = 1; i <= 4; ++i) {
          const char h = buf_[pos + i];
          int digit = 0;
          if (h >= '0' && h <= '9') {
            digit = h - '0';
          } else if (h >= 'a' && h <= 'f') {
            digit = h - 'a' + 10;
          } else if (h >= 'A' && h <= 'F') {
            digit = h - 'A' + 10;
          } else {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          code = code * 16 + digit;
        }
        if (OB_FAIL(ret)) {
          break;
        }
        pos += 4;
        // encode the code point as UTF-8
        if (code < 0x80) {
          out.push_back(static_cast<char>(code));
        } else if (code < 0x800) {
          out.push_back(static_cast<char>(0xC0 | (code >> 6)));
          out.push_back(static_cast<char>(0x80 | (code & 0x3F)));
        } else {
          out.push_back(static_cast<char>(0xE0 | (code >> 12)));
          out.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3F)));
          out.push_back(static_cast<char>(0x80 | (code & 0x3F)));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      if (OB_FAIL(ret)) {
        break;
      }
      ++pos;
    } else if ('"' == c) {
      ++pos;
      break;
    } else {
      out.push_back(c);
      ++pos;
    }
  }
  return ret;
}

int ObFileJsonReader::parse_number(int64_t &pos, int64_t end, bool &is_integer, std::string &out)
{
  int ret = OB_SUCCESS;
  is_integer = true;
  out.clear();
  const int64_t begin = pos;
  while (pos < end) {
    const char c = buf_[pos];
    if ('-' == c || '+' == c || (c >= '0' && c <= '9')) {
      ++pos;
    } else if ('.' == c || 'e' == c || 'E' == c) {
      is_integer = false;
      ++pos;
    } else {
      break;
    }
  }
  if (pos == begin) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    out.assign(&buf_[begin], pos - begin);
  }
  return ret;
}

int ObFileJsonReader::add_field_name(const common::ObString &name, int64_t &index)
{
  int ret = OB_SUCCESS;
  index = -1;
  for (size_t i = 0; i < field_names_.size(); ++i) {
    if (0 == field_names_[i].case_compare(name)) {
      index = static_cast<int64_t>(i);
      break;
    }
  }
  if (index < 0) {
    if (!collect_names_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      name_pool_.push_back(std::string(name.ptr(), name.length()));
      const std::string &stored = name_pool_.back();
      field_names_.push_back(common::ObString(static_cast<int64_t>(stored.length()), stored.data()));
      index = static_cast<int64_t>(field_names_.size()) - 1;
    }
  }
  return ret;
}

int ObFileJsonReader::parse_object(int64_t begin, int64_t end, ObFileScanSink &sink)
{
  int ret = OB_SUCCESS;
  int64_t pos = begin;
  std::string key;
  std::string value;
  scratch_.clear();
  std::vector<int64_t> value_off;
  std::vector<int32_t> value_len;
  std::vector<char> value_null;
  std::vector<ObFileScanValueKind> value_kind;
  value_off.reserve(field_names_.size());
  value_len.reserve(field_names_.size());
  value_null.reserve(field_names_.size());

  if (OB_FAIL(skip_ws(pos, end))) {
    return ret;
  } else if (pos >= end || '{' != buf_[pos]) {
    ret = OB_ERR_UNEXPECTED;
    return ret;
  }
  ++pos;
  if (OB_FAIL(skip_ws(pos, end))) {
    return ret;
  }
  if (pos < end && '}' == buf_[pos]) {
    ++pos;
  } else {
    while (true) {
      if (OB_FAIL(skip_ws(pos, end))) {
        return ret;
      }
      if (OB_FAIL(parse_string(pos, end, key))) {
        return ret;
      }
      if (OB_FAIL(skip_ws(pos, end))) {
        return ret;
      }
      if (pos >= end || ':' != buf_[pos]) {
        ret = OB_ERR_UNEXPECTED;
        return ret;
      }
      ++pos;
      if (OB_FAIL(skip_ws(pos, end))) {
        return ret;
      }
      if (pos >= end) {
        ret = OB_ERR_UNEXPECTED;
        return ret;
      }
      const char c = buf_[pos];
      if ('{' == c || '[' == c) {
        // nested structures are not supported
        ret = OB_ERR_UNEXPECTED;
        return ret;
      }
      bool is_null = false;
      bool is_integer = false;
      ObFileScanValueKind kind = ObFileScanValueKind::TEXT;
      if ('"' == c) {
        if (OB_FAIL(parse_string(pos, end, value))) {
          return ret;
        }
        kind = ObFileScanValueKind::JSON_STRING;
      } else if ('t' == c || 'f' == c) {
        const bool is_true = ('t' == c);
        const int64_t expect_len = is_true ? 4 : 5;
        if (pos + expect_len > end || 0 != memcmp(&buf_[pos], is_true ? "true" : "false", expect_len)) {
          ret = OB_ERR_UNEXPECTED;
          return ret;
        }
        pos += expect_len;
        value.assign(is_true ? "true" : "false");
        kind = ObFileScanValueKind::JSON_BOOL;
      } else if ('n' == c) {
        if (pos + 4 > end || 0 != memcmp(&buf_[pos], "null", 4)) {
          ret = OB_ERR_UNEXPECTED;
          return ret;
        }
        pos += 4;
        is_null = true;
        kind = ObFileScanValueKind::JSON_NULL;
      } else {
        if (OB_FAIL(parse_number(pos, end, is_integer, value))) {
          return ret;
        }
        kind = is_integer ? ObFileScanValueKind::JSON_INT : ObFileScanValueKind::JSON_DOUBLE;
      }

      int64_t index = -1;
      if (OB_FAIL(add_field_name(common::ObString(key.size(), const_cast<char *>(key.data())), index))) {
        return ret;
      }
      if (index >= static_cast<int64_t>(value_off.size())) {
        value_off.resize(index + 1, -1);
        value_len.resize(index + 1, 0);
        value_null.resize(index + 1, 1);
        value_kind.resize(index + 1, ObFileScanValueKind::TEXT);
      }
      const int64_t value_begin = static_cast<int64_t>(scratch_.size());
      if (!is_null) {
        scratch_.append(value.data(), value.size());
      }
      value_off[index] = value_begin;
      value_len[index] = is_null ? 0 : static_cast<int32_t>(value.size());
      value_null[index] = is_null ? 1 : 0;
      value_kind[index] = kind;

      if (OB_FAIL(skip_ws(pos, end))) {
        return ret;
      }
      if (pos >= end) {
        ret = OB_ERR_UNEXPECTED;
        return ret;
      }
      if (',' == buf_[pos]) {
        ++pos;
        continue;
      }
      if ('}' == buf_[pos]) {
        ++pos;
        break;
      }
      ret = OB_ERR_UNEXPECTED;
      return ret;
    }
  }
  if (OB_FAIL(skip_ws(pos, end))) {
    return ret;
  } else if (pos != end) {
    ret = OB_ERR_UNEXPECTED;
    return ret;
  }

  // materialize the row
  // A record does not hold every field of the schema, the fields that are
  // missing are null.
  values_.clear();
  for (size_t i = 0; i < field_names_.size(); ++i) {
    common::ObString value;
    const bool has_value = (i < value_off.size()) && (0 == value_null[i]);
    if (has_value) {
      value.assign_ptr(scratch_.data() + value_off[i], value_len[i]);
    }
    const ObFileScanValueKind kind = (i < value_kind.size()) ? value_kind[i] : ObFileScanValueKind::TEXT;
    values_.push_back(ObFileScanValue(value, !has_value, kind));
  }
  if (OB_FAIL(sink.on_row(values_.data(), values_.size()))) {
    return ret;
  } else {
    ++record_count_;
  }
  return ret;
}

int ObFileJsonReader::scan_records(bool is_end, ObFileScanSink &sink)
{
  int ret = OB_SUCCESS;
  while (true) {
    if (head_ >= tail_) {
      break;
    }
    int64_t line_end = head_;
    while (line_end < tail_ && '\n' != buf_[line_end]) {
      ++line_end;
    }
    if (line_end >= tail_ && !is_end) {
      break;
    }
    int64_t content_begin = head_;
    int64_t content_end = line_end;
    if (content_end > content_begin && '\r' == buf_[content_end - 1]) {
      --content_end;
    }
    int64_t scan = content_begin;
    while (scan < content_end && (' ' == buf_[scan] || '\t' == buf_[scan])) {
      ++scan;
    }
    if (scan >= content_end) {
      // a blank line
      head_ = (line_end < tail_) ? line_end + 1 : line_end;
      continue;
    }
    if (OB_FAIL(parse_object(scan, content_end, sink))) {
      return ret;
    }
    head_ = (line_end < tail_) ? line_end + 1 : line_end;
  }
  return ret;
}

int ObFileJsonReader::feed(const char *data, int64_t length, bool is_end, ObFileScanSink &sink)
{
  int ret = OB_SUCCESS;
  if (length > 0) {
    if (NULL == data) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      if (head_ > 0) {
        if (tail_ > head_) {
          memmove(&buf_[0], &buf_[head_], tail_ - head_);
        }
        tail_ -= head_;
        head_ = 0;
      }
      if (tail_ + length > static_cast<int64_t>(buf_.size())) {
        buf_.resize(tail_ + length);
      }
      memcpy(&buf_[tail_], data, length);
      tail_ += length;
      if (0 == record_count_ && tail_ >= 3 && 0xEF == (unsigned char)buf_[0]
          && 0xBB == (unsigned char)buf_[1] && 0xBF == (unsigned char)buf_[2]) {
        memmove(&buf_[0], &buf_[3], tail_ - 3);
        tail_ -= 3;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scan_records(is_end, sink))) {
    } else if (is_end) {
      head_ = tail_;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
