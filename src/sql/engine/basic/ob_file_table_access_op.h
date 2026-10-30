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

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_file_parquet_reader.h"
#include "sql/engine/basic/ob_file_scan_utils.h"
#include "sql/engine/basic/ob_file_source.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief The specification of the operator that reads a local file.
 */
class ObFileTableAccessSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);

public:
  ObFileTableAccessSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
      : ObOpSpec(alloc, type),
        column_exprs_(alloc),
        column_types_(alloc),
        column_names_(alloc),
        path_(),
        null_str_(),
        delimiter_(),
        quote_(),
        escape_(),
        format_(0),
        header_(true),
        file_list_(alloc),
        described_names_(alloc),
        described_types_(alloc),
        described_nullable_(alloc)
  {}

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(format), K_(header));

  common::ObFixedArray<ObExpr *, common::ObIAllocator> column_exprs_;
  // the ObFileScanType of every column
  common::ObFixedArray<int32_t, common::ObIAllocator> column_types_;
  // the name of every column
  common::ObFixedArray<common::ObString, common::ObIAllocator> column_names_;
  common::ObString path_;
  common::ObString null_str_;
  common::ObString delimiter_;
  common::ObString quote_;
  common::ObString escape_;
  int32_t format_;
  bool header_;
  // the files that were found by the glob function
  common::ObFixedArray<common::ObString, common::ObIAllocator> file_list_;
  // the columns of the file when the statement describes them
  common::ObFixedArray<common::ObString, common::ObIAllocator> described_names_;
  common::ObFixedArray<int32_t, common::ObIAllocator> described_types_;
  common::ObFixedArray<int32_t, common::ObIAllocator> described_nullable_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFileTableAccessSpec);
};

/**
 * @brief The operator that reads a local file.
 *
 * The operator is the leaf of the plan, the rows of the file are the rows of
 * the operator.  The file is read in chunks, it is never materialized in
 * memory as a whole.
 */
class ObFileTableAccessOp : public ObOperator, public ObFileScanSink
{
public:
  ObFileTableAccessOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObFileTableAccessOp();
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int switch_iterator() override;
  virtual int inner_get_next_row() override;
  virtual int inner_close() override;
  virtual void destroy() override { ObOperator::destroy(); }

  /// the sink of the readers of the formats
  virtual int on_row(const ObFileScanValue *values, int64_t count) override;

private:
  int open_next_file();
  int read_next_record();
  int read_next_parquet_record();
  int read_next_glob_record();
  int read_next_describe_record();
  int set_row_datum(int64_t index, const common::ObString &value, bool is_null);
  int set_row_datum(int64_t index, const ObParquetValue &value);
  int set_null_datum(int64_t index);
  int build_scan_options(ObFileScanOptions &options);
  int read_file_chunk(ObFileScanReader &reader);
  int consume_pending_row();

private:
  common::ObIAllocator *allocator_;
  ObFileCsvReader csv_reader_;
  ObFileJsonReader json_reader_;
  ObFileParquetReader parquet_reader_;
  ObFileTableFileSource *file_source_;
  // the reader of the file, the file is read once and the records are consumed
  // one after the other
  common::ObFileReader file_reader_;
  int64_t file_offset_;
  int64_t file_size_;
  bool is_file_opened_;
  bool is_file_end_;
  // the records that were read but not used yet
  std::vector<ObFileScanValue> pending_values_;
  // the strings of the records that wait for the consumer
  std::vector<std::string> pending_strings_;
  std::vector<int64_t> pending_row_ends_;
  // the row of the description that is produced next
  int64_t describe_row_index_;
  int64_t pending_row_index_;
  int64_t pending_row_count_;
  // the values of the columns of the current row group of a parquet file
  std::vector<std::vector<ObParquetValue> > parquet_group_columns_;
  int64_t file_index_;
  int64_t row_group_index_;
  int64_t row_index_in_group_;
  int64_t row_count_in_group_;
  bool is_eof_;
  bool is_opened_;
};

} // namespace sql
} // namespace oceanbase
