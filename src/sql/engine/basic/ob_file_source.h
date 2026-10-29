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

#include "lib/allocator/ob_allocator.h"
#include "lib/file/ob_file.h"
#include "lib/string/ob_string.h"
#include "sql/engine/basic/ob_file_parquet_reader.h"
#include "sql/engine/basic/ob_file_scan_utils.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief A byte source that reads a local file.
 *
 * The file is opened once and the requested byte ranges are read on demand,
 * the file is never materialized in memory as a whole.
 */
class ObFileTableFileSource : public ObParquetFileSource
{
public:
  ObFileTableFileSource();
  ~ObFileTableFileSource();
  int open(const common::ObString &path);
  virtual int64_t get_size() const override { return size_; }
  virtual int read(int64_t offset, int64_t length, char *buf) override;

private:
  common::ObFileReader reader_;
  int64_t size_;
  bool is_opened_;
};

/**
 * @brief Read a local file and feed its content to a reader.
 *
 * The file is read in chunks, the reader never holds more than one chunk of
 * the file in memory.
 */
int ob_file_table_read_chunks(common::ObIAllocator &allocator,
                              const common::ObString &path,
                              ObFileScanReader &reader,
                              ObFileScanSink &sink);

} // namespace sql
} // namespace oceanbase
