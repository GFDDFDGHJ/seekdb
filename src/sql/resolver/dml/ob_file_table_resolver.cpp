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

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/engine/basic/ob_file_parquet_reader.h"
#include "sql/engine/basic/ob_file_scan_utils.h"
#include "sql/engine/basic/ob_file_source.h"
#include "sql/resolver/ob_resolver_utils.h"

#include <cstring>

#ifdef _WIN32
#include <windows.h>
#else
#include <glob.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace oceanbase
{
namespace sql
{

namespace
{

/// the maximum length of a path
const int64_t MAX_PATH_LENGTH = 4096;

/// the length of a varchar column of a file table
const int16_t FILE_TABLE_VARCHAR_LENGTH = 4096;

/**
 * @brief Check whether the path holds a wildcard.
 */
bool path_has_wildcard(const common::ObString &path)
{
  static const char wildcards[] = "?*[]";
  for (int64_t i = 0; i < path.length(); ++i) {
    for (size_t j = 0; j < sizeof(wildcards) - 1; ++j) {
      if (path.ptr()[i] == wildcards[j]) {
        return true;
      }
    }
  }
  return false;
}

/**
 * @brief Expand a wildcard pattern into the list of the matching files.
 */
int expand_glob(const common::ObString &pattern, std::vector<common::ObString> &files)
{
  int ret = OB_SUCCESS;
  if (pattern.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
#ifdef _WIN32
    common::ObMalloc allocator;
    char *path = NULL;
    if (OB_ISNULL(path = static_cast<char *>(allocator.alloc(MAX_PATH_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(path, pattern.ptr(), pattern.length());
      path[pattern.length()] = '\0';
      WIN32_FIND_DATAA find_data;
      HANDLE handle = FindFirstFileA(path, &find_data);
      if (INVALID_HANDLE_VALUE != handle) {
        do {
          if (0 == (find_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
            common::ObString file(find_data.cFileName);
            files.push_back(file);
          }
        } while (FindNextFileA(handle, &find_data));
        FindClose(handle);
      }
      allocator.free(path);
    }
#else
    glob_t glob_result;
    MEMSET(&glob_result, 0, sizeof(glob_result));
    common::ObMalloc allocator;
    char *c_pattern = NULL;
    if (OB_ISNULL(c_pattern = static_cast<char *>(allocator.alloc(pattern.length() + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(c_pattern, pattern.ptr(), pattern.length());
      c_pattern[pattern.length()] = '\0';
      const int return_value = glob(c_pattern, 0, NULL, &glob_result);
      if (GLOB_NOMATCH == return_value) {
        // no file matches the pattern
      } else if (0 != return_value) {
        ret = OB_ERR_SYS;
        LOG_WARN("fail to expand the wildcard pattern", K(ret));
      } else {
        for (size_t i = 0; OB_SUCC(ret) && i < glob_result.gl_pathc; ++i) {
          common::ObString file(glob_result.gl_pathv[i]);
          files.push_back(file);
        }
      }
      globfree(&glob_result);
      allocator.free(c_pattern);
    }
#endif
  }
  return ret;
}

/**
 * @brief Map the type of a parquet column onto the type of a file column.
 */
ObFileScanType parquet_type_to_scan_type(ObParquetType type)
{
  ObFileScanType result = ObFileScanType::VARCHAR;
  switch (type) {
  case ObParquetType::BOOLEAN:
    result = ObFileScanType::BOOLEAN;
    break;
  case ObParquetType::INT32:
  case ObParquetType::INT64:
    result = ObFileScanType::BIGINT;
    break;
  case ObParquetType::FLOAT:
  case ObParquetType::DOUBLE:
    result = ObFileScanType::DOUBLE;
    break;
  default:
    result = ObFileScanType::VARCHAR;
    break;
  }
  return result;
}

/**
 * @brief Map the type of a file column onto the type of a column of the table.
 */
common::ObObjType scan_type_to_obj_type(ObFileScanType type)
{
  common::ObObjType result = common::ObVarcharType;
  switch (type) {
  case ObFileScanType::BIGINT:
    result = common::ObIntType;
    break;
  case ObFileScanType::DOUBLE:
    result = common::ObDoubleType;
    break;
  case ObFileScanType::BOOLEAN:
    result = common::ObTinyIntType;
    break;
  case ObFileScanType::DATE:
    result = common::ObDateType;
    break;
  case ObFileScanType::DATETIME:
    result = common::ObDateTimeType;
    break;
  case ObFileScanType::VARCHAR:
  case ObFileScanType::UNKNOWN:
  default:
    result = common::ObVarcharType;
    break;
  }
  return result;
}

} // namespace

/**
 * @brief Read a local file and expose it as a table.
 *
 * The file is inspected when the statement is resolved, the discovered schema
 * becomes the schema of the table.
 */
int ObDMLResolver::resolve_file_table_item(const ParseNode &table_node, TableItem *&table_item)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *stmt = get_stmt();
  TableItem *new_table_item = NULL;
  const ParseNode *func_name_node = NULL;
  const ParseNode *args_node = NULL;
  const ParseNode *alias_node = NULL;
  common::ObString func_name;
  common::ObString alias_name;
  bool enable_file_sql = false;
  ObFileScanOptions options;
  void *buf = NULL;

  if (OB_ISNULL(stmt) || OB_ISNULL(params_.allocator_) || OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(params_.session_info_->get_enable_file_sql(enable_file_sql))) {
  } else if (!enable_file_sql) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "the file table functions");
  } else if (T_FILE_TABLE_EXPRESSION != table_node.type_ || 3 != table_node.num_child_) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    func_name_node = table_node.children_[0];
    args_node = table_node.children_[1];
    alias_node = table_node.children_[2];
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(func_name_node)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      func_name.assign(const_cast<char *>(func_name_node->str_value_), func_name_node->str_len_);
      // the rewrite of the DESCRIBE statement marks the node it describes
      options.is_describe_ = (1 == table_node.value_);
    }
  }
  if (OB_SUCC(ret) && !OB_ISNULL(alias_node)) {
    alias_name.assign(const_cast<char *>(alias_node->str_value_), alias_node->str_len_);
  }
  if (OB_SUCC(ret)) {
    ret = resolve_file_table_args(args_node, func_name, options);
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(new_table_item = stmt->create_table_item(*params_.allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create table item failed");
    } else if (OB_ISNULL(buf = params_.allocator_->alloc(sizeof(ObFileTableDef)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      new_table_item->file_table_def_ = new (buf) ObFileTableDef();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_file_table_def(*new_table_item->file_table_def_, options))) {
    } else if (options.is_describe_) {
      ret = build_file_table_describe_columns(*new_table_item->file_table_def_);
    }
  }
  if (OB_SUCC(ret) && !OB_ISNULL(params_.query_ctx_)) {
    // the content of the file may change, the plan of the statement must not be cached
    params_.query_ctx_->set_has_file_table(true);
  }
  if (OB_SUCC(ret)) {
    new_table_item->table_id_ = generate_table_id();
    if (!alias_name.empty()) {
      new_table_item->table_name_ = alias_name;
      new_table_item->alias_name_ = alias_name;
    } else if (OB_FAIL(stmt->generate_func_table_name(*params_.allocator_, new_table_item->table_name_))) {
    } else {
      new_table_item->alias_name_ = new_table_item->table_name_;
    }
    new_table_item->type_ = TableItem::FILE_TABLE;
    new_table_item->is_view_table_ = false;
    if (OB_FAIL(stmt->add_table_item(params_.session_info_, new_table_item))) {
    } else if (OB_FAIL(gen_file_table_column_items(*new_table_item))) {
    } else {
      table_item = new_table_item;
    }
  }
  return ret;
}

/**
 * @brief Parse the arguments of a file table function.
 *
 * The first argument is the path of the file, the other arguments are named
 * parameters such as ``header = true``.
 */
int ObDMLResolver::resolve_file_table_args(const ParseNode *args_node,
                                           const common::ObString &func_name,
                                           ObFileScanOptions &options)
{
  int ret = OB_SUCCESS;
  int64_t positional_count = 0;
  if (0 == func_name.case_compare("read_csv")) {
    options.format_ = ObFileScanFormat::CSV;
  } else if (0 == func_name.case_compare("read_json") || 0 == func_name.case_compare("read_ndjson")) {
    options.format_ = ObFileScanFormat::JSONL;
  } else if (0 == func_name.case_compare("read_parquet")) {
    options.format_ = ObFileScanFormat::PARQUET;
  } else if (0 == func_name.case_compare("glob")) {
    options.format_ = ObFileScanFormat::GLOB;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, func_name.ptr());
  }
  if (OB_SUCC(ret) && (OB_ISNULL(args_node) || args_node->num_child_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "the file table function needs the path of a file");
  }
  for (int64_t i = 0; OB_SUCC(ret) && !OB_ISNULL(args_node) && i < args_node->num_child_; ++i) {
    const ParseNode *arg = args_node->children_[i];
    if (OB_ISNULL(arg)) {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    common::ObString option_name;
    const ParseNode *value_node = NULL;
    if (T_OP_EQ == arg->type_ && 2 == arg->num_child_) {
      // a named parameter
      if (OB_FAIL(extract_file_table_option_name(arg->children_[0], option_name))) {
        break;
      }
      value_node = arg->children_[1];
    } else {
      // a positional parameter, only the path of the file is allowed
      ++positional_count;
      if (1 == positional_count) {
        value_node = arg;
      } else {
        ret = OB_ERR_PARSE_SQL;
        LOG_USER_ERROR(OB_ERR_PARSE_SQL,
                       ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX),
                       static_cast<int32_t>(func_name.length()),
                       func_name.ptr(),
                       1);
        break;
      }
    }
    if (OB_SUCC(ret)) {
      ret = apply_file_table_option(option_name, value_node, options);
    }
  }
  if (OB_SUCC(ret) && options.path_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "the file table function needs the path of a file");
  }
  if (OB_SUCC(ret) && !options.has_header_) {
    // the header of a csv file is used by default
    options.header_ = (ObFileScanFormat::CSV == options.format_);
  }
  return ret;
}

/**
 * @brief Extract the name of a named parameter.
 */
int ObDMLResolver::extract_file_table_option_name(const ParseNode *node, common::ObString &name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (T_COLUMN_REF == node->type_ && 3 == node->num_child_ && OB_ISNULL(node->children_[0])
             && OB_ISNULL(node->children_[1]) && !OB_ISNULL(node->children_[2])) {
    name.assign(const_cast<char *>(node->children_[2]->str_value_), node->children_[2]->str_len_);
  } else {
    ret = OB_ERR_PARSE_SQL;
    LOG_USER_ERROR(OB_ERR_PARSE_SQL,
                   ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX),
                   static_cast<int32_t>(name.length()),
                   name.ptr(),
                   1);
  }
  return ret;
}

/**
 * @brief Apply a named parameter of a file table function.
 */
int ObDMLResolver::apply_file_table_option(const common::ObString &option_name,
                                            const ParseNode *value_node,
                                            ObFileScanOptions &options)
{
  int ret = OB_SUCCESS;
  if (option_name.empty()) {
    // the positional parameter, the path of the file
    ret = extract_file_table_string(value_node, options.path_);
  } else if (0 == option_name.case_compare("header")) {
    bool value = false;
    if (OB_FAIL(extract_file_table_bool(value_node, value))) {
    } else {
      options.has_header_ = true;
      options.header_ = value;
    }
  } else if (0 == option_name.case_compare("sample_size")) {
    int64_t value = 0;
    if (OB_FAIL(extract_file_table_int(value_node, value))) {
    } else {
      options.sample_size_ = value;
    }
  } else if (0 == option_name.case_compare("format")) {
    ret = extract_file_table_string(value_node, options.json_format_);
  } else if (0 == option_name.case_compare("nullstr")) {
    ret = extract_file_table_string(value_node, options.null_str_);
  } else if (0 == option_name.case_compare("delimiter") || 0 == option_name.case_compare("sep")) {
    ret = extract_file_table_string(value_node, options.delimiter_);
  } else if (0 == option_name.case_compare("quote")) {
    ret = extract_file_table_string(value_node, options.quote_);
  } else if (0 == option_name.case_compare("escape")) {
    ret = extract_file_table_string(value_node, options.escape_);
  } else if (0 == option_name.case_compare("ignore_errors")) {
    bool value = false;
    if (OB_FAIL(extract_file_table_bool(value_node, value))) {
    } else {
      options.ignore_errors_ = value;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, option_name.ptr());
  }
  return ret;
}

/**
 * @brief Find the constant node of a parse node.
 *
 * The plan cache turns the constants of a statement into parameters before the
 * statement is resolved. The value of a string stays in the child of the node,
 * the value of a number is kept in the parameter store.
 */
const ParseNode *ObDMLResolver::get_file_table_const_node(const ParseNode *node)
{
  const ParseNode *result = node;
  if (!OB_ISNULL(node) && T_QUESTIONMARK == node->type_) {
    result = NULL;
    for (int64_t i = 0; OB_ISNULL(result) && i < node->num_child_; ++i) {
      const ParseNode *child = node->children_[i];
      if (!OB_ISNULL(child) && T_QUESTIONMARK != child->type_) {
        result = child;
      }
    }
  }
  return result;
}

/**
 * @brief Extract a string value from a parse node.
 */
int ObDMLResolver::extract_file_table_string(const ParseNode *node, common::ObString &value)
{
  int ret = OB_SUCCESS;
  const ParseNode *literal = get_file_table_const_node(node);
  if (OB_ISNULL(literal) || (T_VARCHAR != literal->type_ && T_CHAR != literal->type_)) {
    ret = OB_ERR_PARSE_SQL;
    LOG_USER_ERROR(OB_ERR_PARSE_SQL, ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX), 0, "", 1);
  } else {
    value.assign(const_cast<char *>(literal->str_value_), literal->str_len_);
  }
  return ret;
}

/**
 * @brief Extract a boolean value from a parse node.
 */
int ObDMLResolver::extract_file_table_bool(const ParseNode *node, bool &value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (T_BOOL == node->type_ || T_INT == node->type_) {
    value = (0 != node->value_);
  } else {
    ret = OB_ERR_PARSE_SQL;
    LOG_USER_ERROR(OB_ERR_PARSE_SQL, ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX), 0, "", 1);
  }
  return ret;
}

/**
 * @brief Extract an integer value from a parse node.
 */
int ObDMLResolver::extract_file_table_int(const ParseNode *node, int64_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (T_INT == node->type_) {
    value = node->value_;
  } else if (T_UINT64 == node->type_) {
    value = static_cast<int64_t>(node->value_);
  } else if (T_OP_NEG == node->type_ && 1 == node->num_child_ && !OB_ISNULL(node->children_[0])
             && T_INT == node->children_[0]->type_) {
    value = -node->children_[0]->value_;
  } else {
    ret = OB_ERR_PARSE_SQL;
    LOG_USER_ERROR(OB_ERR_PARSE_SQL, ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX), 0, "", 1);
  }
  return ret;
}

/**
 * @brief Turn the columns of the file into the rows of a description.
 *
 * The description reports six columns for every column of the file, so the
 * columns of the file are kept apart and the table gets the six columns of the
 * description instead.
 */
int ObDMLResolver::build_file_table_describe_columns(ObFileTableDef &table_def)
{
  int ret = OB_SUCCESS;
  static const char *const names[] = {"column_name", "column_type", "null", "key", "default", "extra"};
  for (int64_t i = 0; OB_SUCC(ret) && i < table_def.columns_.count(); ++i) {
    if (OB_FAIL(table_def.described_columns_.push_back(table_def.columns_.at(i)))) {
    }
  }
  if (OB_SUCC(ret)) {
    table_def.columns_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < 6; ++i) {
      ObFileTableDef::Column column;
      column.name_ = common::ObString::make_string(names[i]);
      column.type_ = static_cast<int32_t>(ObFileScanType::VARCHAR);
      column.nullable_ = true;
      if (OB_FAIL(table_def.columns_.push_back(column))) {
      }
    }
  }
  if (OB_SUCC(ret)) {
    table_def.format_ = static_cast<int32_t>(ObFileScanFormat::DESCRIBE);
    table_def.row_count_ = table_def.described_columns_.count();
  }
  return ret;
}

/**
 * @brief Check that the path of the file is allowed and inspect the file.
 */
int ObDMLResolver::resolve_file_table_def(ObFileTableDef &table_def, const ObFileScanOptions &options)
{
  int ret = OB_SUCCESS;
  if (options.path_.length() >= MAX_PATH_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "the path of the file is too long");
  } else if (ObFileScanFormat::GLOB == options.format_) {
    ret = resolve_file_table_glob(table_def, options);
  } else {
    ret = resolve_file_table_file(table_def, options);
  }
  return ret;
}

/**
 * @brief Expand the wildcard of the glob function and check the files.
 */
int ObDMLResolver::resolve_file_table_glob(ObFileTableDef &table_def, const ObFileScanOptions &options)
{
  int ret = OB_SUCCESS;
  std::vector<common::ObString> files;
  if (path_has_wildcard(options.path_)) {
    ret = expand_glob(options.path_, files);
  } else if (OB_FAIL(check_file_table_path(options.path_))) {
    ret = OB_FILE_NOT_EXIST;
  } else {
    files.push_back(options.path_);
    ret = OB_SUCCESS;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(files.size()); ++i) {
    if (OB_FAIL(check_file_table_path(files[i]))) {
      break;
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(files.size()); ++i) {
      common::ObString file;
      if (OB_FAIL(dup_file_table_string(files[i], file))) {
        break;
      }
      if (OB_FAIL(table_def.files_.push_back(file))) {
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    // the glob function always returns a single column
    ObFileTableDef::Column column;
    column.name_ = "file";
    column.type_ = static_cast<int32_t>(ObFileScanType::VARCHAR);
    column.nullable_ = false;
    ret = table_def.columns_.push_back(column);
  }
  if (OB_SUCC(ret)) {
    table_def.format_ = static_cast<int32_t>(options.format_);
    table_def.row_count_ = static_cast<int64_t>(files.size());
    ret = dup_file_table_string(options.path_, table_def.path_);
  }
  return ret;
}

/**
 * @brief Check that the file may be read and inspect its content.
 */
int ObDMLResolver::resolve_file_table_file(ObFileTableDef &table_def, const ObFileScanOptions &options)
{
  int ret = OB_SUCCESS;
  common::ObString real_path;
  if (options.sample_size_ != -1) {
    // only the whole file is inspected
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "the sample_size parameter");
  } else if (OB_FAIL(resolve_file_table_real_path(options.path_, real_path))) {
  } else if (OB_FAIL(check_file_table_path(real_path))) {
  } else if (ObFileScanFormat::CSV == options.format_) {
    ret = infer_file_table_csv(table_def, options, real_path);
  } else if (ObFileScanFormat::JSONL == options.format_) {
    ret = infer_file_table_json(table_def, options, real_path);
  } else if (ObFileScanFormat::PARQUET == options.format_) {
    ret = infer_file_table_parquet(table_def, options, real_path);
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  if (OB_SUCC(ret)) {
    table_def.format_ = static_cast<int32_t>(options.format_);
    table_def.header_ = options.header_;
    table_def.sample_size_ = options.sample_size_;
    if (OB_FAIL(dup_file_table_string(real_path, table_def.path_))) {
    } else if (OB_FAIL(dup_file_table_string(options.null_str_, table_def.null_str_))) {
    } else if (OB_FAIL(dup_file_table_string(options.delimiter_, table_def.delimiter_))) {
    } else if (OB_FAIL(dup_file_table_string(options.quote_, table_def.quote_))) {
    } else if (OB_FAIL(dup_file_table_string(options.escape_, table_def.escape_))) {
    }
  }
  return ret;
}

/**
 * @brief Check that the path of the file is inside the secure file priv.
 */
int ObDMLResolver::check_file_table_path(const common::ObString &path)
{
  int ret = OB_SUCCESS;
  common::ObString secure_file_priv;
  char *path_buf = NULL;
  char *full_path_buf = NULL;
  char *actual_path = NULL;
  if (path.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "the path of the file is empty");
  } else if (path.length() >= MAX_PATH_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "the path of the file is too long");
  } else if (OB_ISNULL(path_buf = static_cast<char *>(params_.allocator_->alloc(MAX_PATH_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(full_path_buf = static_cast<char *>(params_.allocator_->alloc(MAX_PATH_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(params_.session_info_->get_secure_file_priv(secure_file_priv))) {
  } else {
    MEMCPY(path_buf, path.ptr(), path.length());
    path_buf[path.length()] = '\0';
#ifdef _WIN32
    if (OB_ISNULL(actual_path = _fullpath(full_path_buf, path_buf, MAX_PATH_LENGTH))) {
#else
    if (OB_ISNULL(actual_path = realpath(path_buf, full_path_buf))) {
#endif
      ret = OB_FILE_NOT_EXIST;
      LOG_USER_ERROR(OB_FILE_NOT_EXIST, path.ptr());
    } else if (!params_.session_info_->is_inner()
               && OB_FAIL(ObResolverUtils::check_secure_path(secure_file_priv,
                                                            common::ObString(actual_path)))) {
      ret = OB_ERR_NO_PRIVILEGE;
    }
  }
  return ret;
}

/**
 * @brief Resolve the real path of the file.
 */
int ObDMLResolver::resolve_file_table_real_path(const common::ObString &path, common::ObString &real_path)
{
  int ret = OB_SUCCESS;
  char *path_buf = NULL;
  char *full_path_buf = NULL;
  char *actual_path = NULL;
  if (path.empty() || path.length() >= MAX_PATH_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "the path of the file is invalid");
  } else if (OB_ISNULL(path_buf = static_cast<char *>(params_.allocator_->alloc(MAX_PATH_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(full_path_buf = static_cast<char *>(params_.allocator_->alloc(MAX_PATH_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(path_buf, path.ptr(), path.length());
    path_buf[path.length()] = '\0';
#ifdef _WIN32
    if (OB_ISNULL(actual_path = _fullpath(full_path_buf, path_buf, MAX_PATH_LENGTH))) {
#else
    if (OB_ISNULL(actual_path = realpath(path_buf, full_path_buf))) {
#endif
      ret = OB_FILE_NOT_EXIST;
      LOG_USER_ERROR(OB_FILE_NOT_EXIST, path.ptr());
    } else {
      real_path.assign(actual_path, static_cast<int64_t>(strlen(actual_path)));
    }
  }
  return ret;
}

/**
 * @brief Inspect a csv file and build the schema of the table.
 */
int ObDMLResolver::infer_file_table_csv(ObFileTableDef &table_def,
                                        const ObFileScanOptions &options,
                                        const common::ObString &path)
{
  int ret = OB_SUCCESS;
  ObFileCsvReader reader;
  ObFileSchemaInferer inferer;
  std::vector<common::ObString> names;
  std::vector<ObFileScanColumn> columns;
  if (OB_FAIL(reader.init(options, options.header_, 0))) {
  } else if (OB_FAIL(ob_file_table_read_chunks(*params_.allocator_, path, reader, inferer))) {
  } else {
    if (options.header_) {
      names = reader.get_header();
    } else {
      // the names of the columns are generated
      const int64_t count = static_cast<int64_t>(inferer.get_inferers().size());
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        common::ObSqlString name;
        if (OB_FAIL(name.append_fmt("column%ld", i))) {
          break;
        }
        names.push_back(common::ObString(name.length(), name.ptr()));
      }
    }
    if (OB_SUCC(ret)) {
      ret = inferer.build_columns(names, columns);
    }
    if (OB_SUCC(ret)) {
      ret = build_file_table_columns(table_def, names, columns);
    }
  }
  return ret;
}

/**
 * @brief Inspect a json file and build the schema of the table.
 */
int ObDMLResolver::infer_file_table_json(ObFileTableDef &table_def,
                                         const ObFileScanOptions &options,
                                         const common::ObString &path)
{
  int ret = OB_SUCCESS;
  ObFileJsonReader reader;
  ObFileSchemaInferer inferer;
  std::vector<common::ObString> names;
  std::vector<ObFileScanColumn> columns;
  if (!options.json_format_.empty()
      && 0 != options.json_format_.case_compare("newline_delimited")
      && 0 != options.json_format_.case_compare("ndjson")
      && 0 != options.json_format_.case_compare("jsonl")
      && 0 != options.json_format_.case_compare("json")) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, options.json_format_.ptr());
  } else if (OB_FAIL(reader.init(options, NULL))) {
  } else if (OB_FAIL(ob_file_table_read_chunks(*params_.allocator_, path, reader, inferer))) {
  } else {
    names = reader.get_field_names();
    if (OB_SUCC(ret)) {
      ret = inferer.build_columns(names, columns);
    }
    if (OB_SUCC(ret)) {
      ret = build_file_table_columns(table_def, names, columns);
    }
  }
  return ret;
}

/**
 * @brief Read the schema of a parquet file.
 */
int ObDMLResolver::infer_file_table_parquet(ObFileTableDef &table_def,
                                            const ObFileScanOptions &options,
                                            const common::ObString &path)
{
  int ret = OB_SUCCESS;
  ObFileParquetReader reader;
  void *buf = NULL;
  if (OB_ISNULL(buf = params_.allocator_->alloc(sizeof(ObFileTableFileSource)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObFileTableFileSource *source = new (buf) ObFileTableFileSource();
    if (OB_FAIL(source->open(path))) {
      ret = OB_FILE_NOT_EXIST;
      LOG_USER_ERROR(OB_FILE_NOT_EXIST, path.ptr());
    } else if (OB_FAIL(reader.init(source))) {
    } else {
      const std::vector<ObParquetColumnMeta> &columns = reader.get_columns();
      for (size_t i = 0; OB_SUCC(ret) && i < columns.size(); ++i) {
        ObFileTableDef::Column column;
        column.name_.assign(const_cast<char *>(columns[i].name_.data()),
                            static_cast<int64_t>(columns[i].name_.length()));
        column.type_ = static_cast<int32_t>(parquet_type_to_scan_type(columns[i].type_));
        column.nullable_ = (ObParquetRepetition::OPTIONAL == columns[i].repetition_type_);
        if (OB_FAIL(table_def.columns_.push_back(column))) {
          break;
        }
      }
      table_def.row_count_ = reader.get_num_rows();
    }
  }
  return ret;
}

/**
 * @brief Build the columns of the table from the inferred schema.
 */
int ObDMLResolver::build_file_table_columns(ObFileTableDef &table_def,
                                            const std::vector<common::ObString> &names,
                                            const std::vector<ObFileScanColumn> &columns)
{
  int ret = OB_SUCCESS;
  for (size_t i = 0; i < columns.size(); ++i) {
    ObFileTableDef::Column column;
    column.type_ = static_cast<int32_t>(columns[i].type_);
    column.nullable_ = columns[i].nullable_;
    if (OB_FAIL(build_file_table_column_name(names, i, column.name_))) {
      break;
    }
    if (OB_FAIL(table_def.columns_.push_back(column))) {
      break;
    }
  }
  return ret;
}

/**
 * @brief Build the name of a column, the duplicated names are made unique.
 */
int ObDMLResolver::build_file_table_column_name(const std::vector<common::ObString> &names,
                                                 int64_t index,
                                                 common::ObString &name)
{
  int ret = OB_SUCCESS;
  if (index >= static_cast<int64_t>(names.size())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const common::ObString &origin_name = names[index];
    int64_t duplicated = 0;
    for (int64_t i = 0; i < index; ++i) {
      if (0 == names[i].case_compare(origin_name)) {
        ++duplicated;
      }
    }
    if (0 == duplicated) {
      ret = dup_file_table_string(origin_name, name);
    } else {
      common::ObSqlString new_name;
      if (OB_FAIL(new_name.append_fmt("%.*s__%ld", origin_name.length(), origin_name.ptr(), duplicated + 1))) {
      } else {
        ret = dup_file_table_string(common::ObString(new_name.length(), new_name.ptr()), name);
      }
    }
  }
  return ret;
}

/**
 * @brief Copy a string into the allocator of the resolver.
 *
 * The strings of the parse tree are released when the statement is resolved,
 * the strings of the definition of the table have to be copied.
 */
int ObDMLResolver::dup_file_table_string(const common::ObString &source, common::ObString &target)
{
  int ret = OB_SUCCESS;
  if (source.empty()) {
    target = common::ObString();
  } else {
    char *buffer = NULL;
    if (OB_ISNULL(buffer = static_cast<char *>(params_.allocator_->alloc(source.length() + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buffer, source.ptr(), source.length());
      buffer[source.length()] = '\0';
      target.assign(buffer, source.length());
    }
  }
  return ret;
}

/**
 * @brief Create the columns of the table.
 */
int ObDMLResolver::gen_file_table_column_items(TableItem &table_item)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *stmt = get_stmt();
  const ObFileTableDef *table_def = table_item.file_table_def_;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_def) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_def->columns_.count(); ++i) {
      const ObFileTableDef::Column &column = table_def->columns_.at(i);
      const ObFileScanType scan_type = static_cast<ObFileScanType>(column.type_);
      ObColumnRefRawExpr *column_expr = NULL;
      ObRawExprResType result_type;
      result_type.set_type(scan_type_to_obj_type(scan_type));
      common::ObAccuracy accuracy;
      if (common::ObVarcharType == result_type.get_type()) {
        accuracy.set_precision(FILE_TABLE_VARCHAR_LENGTH);
        accuracy.set_scale(0);
        // the text of the file uses the collation of the connection, the level
        // of the collation is not set by the type of the column
        common::ObCollationType coll_type = CS_TYPE_INVALID;
        if (OB_FAIL(params_.session_info_->get_collation_connection(coll_type))) {
        } else {
          result_type.set_collation_type(coll_type);
          result_type.set_collation_level(CS_LEVEL_IMPLICIT);
        }
      } else {
        accuracy = common::ObAccuracy::DDL_DEFAULT_ACCURACY[result_type.get_type()];
        result_type.set_collation_type(CS_TYPE_BINARY);
        if (common::ObDateTimeType == result_type.get_type()) {
          // the file holds the date time without the fractional seconds
          accuracy.set_scale(0);
        }
      }
      result_type.set_accuracy(accuracy);
      if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_REF_COLUMN, column_expr))) {
      } else if (OB_ISNULL(column_expr)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        column_expr->set_result_type(result_type);
        column_expr->set_ref_id(table_item.table_id_, i + OB_APP_MIN_COLUMN_ID);
        column_expr->set_column_attr(table_item.get_table_name(), column.name_);
        if (OB_FAIL(column_expr->add_flag(IS_COLUMN))) {
        } else {
          ColumnItem column_item;
          column_item.expr_ = column_expr;
          column_item.table_id_ = column_expr->get_table_id();
          column_item.column_id_ = column_expr->get_column_id();
          column_item.column_name_ = column_expr->get_column_name();
          ret = stmt->add_column_item(column_item);
          if (OB_SUCC(ret) && OB_FAIL(table_item.file_table_def_->column_exprs_.push_back(column_expr))) {
          }
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
