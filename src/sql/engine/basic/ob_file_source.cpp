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

#include "sql/engine/basic/ob_file_source.h"

namespace oceanbase
{
namespace sql
{

namespace
{
/// the size of the buffer that is used to read a file
const int64_t FILE_READ_CHUNK_SIZE = 256 * 1024;
} // namespace

ObFileTableFileSource::ObFileTableFileSource() : size_(0), is_opened_(false) {}

ObFileTableFileSource::~ObFileTableFileSource() {}

int ObFileTableFileSource::open(const common::ObString &path)
{
  int ret = OB_SUCCESS;
  if (is_opened_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(reader_.open(path, false))) {
    ret = OB_FILE_NOT_EXIST;
    LOG_USER_ERROR(OB_FILE_NOT_EXIST, path.ptr());
  } else {
    size_ = common::get_file_size(path.ptr());
    if (size_ < 0) {
      ret = OB_FILE_NOT_EXIST;
      LOG_USER_ERROR(OB_FILE_NOT_EXIST, path.ptr());
    } else {
      is_opened_ = true;
    }
  }
  return ret;
}

int ObFileTableFileSource::read(int64_t offset, int64_t length, char *buf)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
  } else if (offset < 0 || length < 0 || offset + length > size_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (length > 0 && OB_FAIL(reader_.pread(buf, length, offset, read_size))) {
    ret = OB_IO_ERROR;
  }
  return ret;
}

int ob_file_table_read_chunks(common::ObIAllocator &allocator,
                              const common::ObString &path,
                              ObFileScanReader &reader,
                              ObFileScanSink &sink)
{
  int ret = OB_SUCCESS;
  common::ObFileReader file_reader;
  char *buffer = NULL;
  int64_t offset = 0;
  int64_t file_size = 0;
  if (OB_ISNULL(buffer = static_cast<char *>(allocator.alloc(FILE_READ_CHUNK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(file_reader.open(path, false))) {
    ret = OB_FILE_NOT_EXIST;
    LOG_USER_ERROR(OB_FILE_NOT_EXIST, path.ptr());
  } else if ((file_size = common::get_file_size(path.ptr())) < 0) {
    ret = OB_FILE_NOT_EXIST;
    LOG_USER_ERROR(OB_FILE_NOT_EXIST, path.ptr());
  } else {
    while (OB_SUCC(ret) && offset < file_size) {
      int64_t read_size = 0;
      const int64_t want = (file_size - offset) < FILE_READ_CHUNK_SIZE ? (file_size - offset)
                                                                       : FILE_READ_CHUNK_SIZE;
      if (OB_FAIL(file_reader.pread(buffer, want, offset, read_size))) {
        ret = OB_IO_ERROR;
        break;
      }
      if (read_size <= 0) {
        break;
      }
      offset += read_size;
      if (OB_FAIL(reader.feed(buffer, read_size, offset >= file_size, sink))) {
        break;
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
