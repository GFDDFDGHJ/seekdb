/*
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
#include "ob_expr_ai_rerank.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "lib/utility/utility.h"
#include "common/json_type/ob_json_common.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include <cmath>
 
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase 
{
namespace sql 
{

ObExprAIRerank::ObExprAIRerank(common::ObIAllocator &alloc)
: ObFuncExprOperator(alloc, 
                    T_FUN_SYS_AI_RERANK, 
                    N_AI_RERANK, 
                    MORE_THAN_ZERO,
                    NOT_VALID_FOR_GENERATED_COL, 
                    NOT_ROW_DIMENSION) 
{
}

ObExprAIRerank::~ObExprAIRerank() 
{
}

int ObExprAIRerank::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *types_stack,
                                    int64_t param_num,
                                    common::ObExprTypeCtx &type_ctx) const {
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 3 || param_num > 4)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    if (!ob_is_string_tc(types_stack[MODEL_IDX].get_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, model key must be string type");
    } else if (!ob_is_string_tc(types_stack[QUERY_IDX].get_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, query must be string type");
    } else {
      types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      types_stack[QUERY_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
    ObObjType in_type = types_stack[DOCUMENTS_IDX].get_type();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, DOCUMENTS_IDX, N_AI_RERANK))) {
    } else if (ob_is_string_type(in_type) && types_stack[DOCUMENTS_IDX].get_collation_type() != CS_TYPE_BINARY) {
      if (types_stack[DOCUMENTS_IDX].get_charset_type() != CHARSET_UTF8MB4) {
        types_stack[DOCUMENTS_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (param_num == 4) {
      ObObjType doc_key_type = types_stack[DOC_KEY_IDX].get_type();
      if (doc_key_type == ObNullType) {
      } else if (doc_key_type == ObJsonType) {
      } else if (ob_is_string_type(doc_key_type)) {
        types_stack[DOC_KEY_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      } else {  
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, doc key must be string type");
      }
    }

    if (OB_SUCC(ret)) {
      type.set_json();
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
    }
  }
  return ret;
}

int ObExprAIRerank::eval_ai_rerank(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg_model_id = nullptr;
  ObDatum *arg_query = nullptr;
  ObDatum *arg_documents = nullptr;
  ObDatum *arg_doc_key = nullptr;
  if (expr.arg_cnt_ == 4
      ? OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_query, arg_documents, arg_doc_key))
      : OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_query, arg_documents))) {
  } else if (arg_model_id->is_null() || arg_query->is_null() || arg_documents->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, model id or query or documents is null");
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    MultimodeAlloctor allocator(alloc_guard.get_allocator());
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_RERANK));
    const bool object_documents = OB_NOT_NULL(arg_doc_key) && !arg_doc_key->is_null();
    ObString model_id;
    ObString query;
    ObString doc_key;
    ObIJsonBase *document_base = nullptr;
    ObJsonArray *documents = nullptr;
    ObJsonArray *texts = nullptr;
    ObJsonArray *output = nullptr;
    bool is_null = false;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(
          ctx.exec_ctx_, allocator, *arg_model_id, expr.args_[MODEL_IDX]->datum_meta_,
          expr.args_[MODEL_IDX]->obj_meta_.has_lob_header(), model_id))) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
          ctx.exec_ctx_, allocator, *arg_query, expr.args_[QUERY_IDX]->datum_meta_,
          expr.args_[QUERY_IDX]->obj_meta_.has_lob_header(), query))) {
    } else if (model_id.empty() || query.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, model id or query is empty");
    } else if (OB_FAIL(ObJsonExprHelper::get_json_doc(
          expr, ctx, allocator, DOCUMENTS_IDX, document_base, is_null, true))) {
    } else if (is_null || OB_ISNULL(document_base)
               || document_base->json_type() != ObJsonNodeType::J_ARRAY
               || !document_base->is_tree()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, documents must be a JSON array");
    } else {
      documents = static_cast<ObJsonArray *>(document_base);
      if (documents->element_count() == 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, document array is empty");
      }
    }

    if (OB_SUCC(ret) && object_documents) {
      if (expr.args_[DOC_KEY_IDX]->datum_meta_.type_ == ObJsonType) {
        ObIJsonBase *key_base = nullptr;
        if (OB_FAIL(ObJsonExprHelper::get_json_doc(
              expr, ctx, allocator, DOC_KEY_IDX, key_base, is_null, true))) {
        } else if (is_null || OB_ISNULL(key_base) || key_base->json_type() != ObJsonNodeType::J_STRING) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          doc_key.assign_ptr(key_base->get_data(), key_base->get_data_length());
        }
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
            ctx.exec_ctx_, allocator, *arg_doc_key, expr.args_[DOC_KEY_IDX]->datum_meta_,
            expr.args_[DOC_KEY_IDX]->obj_meta_.has_lob_header(), doc_key))) {
      }
      if (OB_SUCC(ret)) {
        ret = get_doc_array_from_documents_array_with_key(allocator, documents, doc_key, texts);
      }
    } else if (OB_SUCC(ret)) {
      texts = documents;
      for (int64_t i = 0; OB_SUCC(ret) && i < texts->element_count(); ++i) {
        const ObJsonNode *text = texts->get_value(i);
        if (OB_ISNULL(text) || text->json_type() != ObJsonNodeType::J_STRING) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, document array elements must be strings");
        }
      }
    }

    ObAIFuncExprInfo *info = nullptr;
    share::ObAiModelEndpointInfo endpoint;
    query::ObIAiEndpointResolver *resolver =
        ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
    ObArray<ObString> headers;
    ObArray<RerankResult> ranked;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, model_id, info))) {
    } else if (OB_ISNULL(info) || OB_ISNULL(resolver)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ObAIFuncUtils::check_info_type_rerank(info))) {
    } else if (OB_FAIL(resolver->resolve_by_model_name(model_id, allocator, endpoint))) {
    } else if (OB_FAIL(ObAIFuncUtils::get_header(allocator, *info, endpoint, headers))) {
    }
    // Both input forms use the same requests and global index normalization.
    const int64_t batch_size = 20;
    for (int64_t start = 0; OB_SUCC(ret) && start < texts->element_count();) {
      const int64_t count = MIN(batch_size, texts->element_count() - start);
      ObJsonArray *batch_documents = nullptr;
      ObJsonArray *batch_results = nullptr;
      if (OB_FAIL(construct_batch_document_array(allocator, texts, start, start + count, batch_documents))) {
      } else if (OB_FAIL(inner_eval_ai_rerank(
            allocator, *info, endpoint, headers, query, batch_documents, batch_results))) {
      } else if (OB_FAIL(append_rerank_batch(batch_results, start, count, ranked))) {
      }
      start += count;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sort_rerank_results(ranked))) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, output))) {
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < ranked.count(); ++i) {
        const RerankResult &item = ranked.at(i);
        ObJsonNode *source = object_documents ? documents->get_value(item.index_) : item.object_;
        ObJsonObject *entry = nullptr;
        if (OB_ISNULL(source) || source->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_INVALID_DATA;
        } else if (OB_ISNULL(entry = static_cast<ObJsonObject *>(source->clone(&allocator, true)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (object_documents) {
          ObJsonNode *score = item.object_->get_value(SCORE_KEY);
          ObJsonNode *score_copy = nullptr;
          if (OB_ISNULL(score) || OB_ISNULL(score_copy = score->clone(&allocator, true))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (OB_FAIL(entry->add(MODEL_SCORE_KEY, score_copy))) {
          }
        } else {
          ObJsonInt *index = nullptr;
          if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, item.index_, index))) {
          } else if (OB_FAIL(entry->add(INDEX_KEY, index))) {
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(output->append(entry))) {
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObString binary;
      if (OB_FAIL(output->get_raw_binary(binary, &allocator))) {
      } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, binary))) {
      }
    }
  }
  return ret;
}

int ObExprAIRerank::get_doc_array_from_documents_array_with_key(
    ObIAllocator &allocator, ObJsonArray *documents, ObString &doc_key, ObJsonArray *&doc_array)
{
  int ret = OB_SUCCESS;
  ObJsonArray *texts = nullptr;
  if (OB_ISNULL(documents) || doc_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, texts))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < documents->element_count(); ++i) {
      ObJsonNode *node = documents->get_value(i);
      ObJsonNode *text = nullptr;
      if (OB_ISNULL(node) || node->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_ISNULL(text = static_cast<ObJsonObject *>(node)->get_value(doc_key))
                 || text->json_type() != ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(texts->append(text))) {
      }
    }
    if (OB_SUCC(ret)) {
      doc_array = texts;
    }
  }
  if (ret == OB_INVALID_ARGUMENT) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, each document must have a string at the selected text key");
  }
  return ret;
}

int ObExprAIRerank::construct_batch_document_array(
    ObIAllocator &allocator, ObJsonArray *documents, int64_t start, int64_t end, ObJsonArray *&batch)
{
  int ret = OB_SUCCESS;
  ObJsonArray *array = nullptr;
  if (OB_ISNULL(documents) || start < 0 || end < start || end > documents->element_count()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, array))) {
  } else {
    for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
      if (OB_FAIL(array->append(documents->get_value(i)))) {
      }
    }
    if (OB_SUCC(ret)) {
      batch = array;
    }
  }
  return ret;
}

int ObExprAIRerank::inner_eval_ai_rerank(
    ObIAllocator &allocator, const ObAIFuncExprInfo &info,
    const share::ObAiModelEndpointInfo &endpoint, ObArray<ObString> &headers,
    ObString &query, ObJsonArray *documents, ObJsonArray *&results)
{
  int ret = OB_SUCCESS;
  ObJsonObject *body = nullptr;
  ObJsonObject *http_response = nullptr;
  ObIJsonBase *response = nullptr;
  ObAIFuncClient client;
  if (OB_FAIL(ObAIFuncUtils::get_rerank_body(allocator, info, endpoint, query, documents, nullptr, body))) {
  } else if (OB_FAIL(client.send_post(allocator, endpoint.get_url(), headers, body, http_response))) {
  } else if (OB_FAIL(ObAIFuncUtils::parse_rerank_output(allocator, endpoint, http_response, response))) {
  } else if (OB_ISNULL(response) || response->json_type() != ObJsonNodeType::J_ARRAY || !response->is_tree()) {
    ret = OB_INVALID_DATA;
  } else {
    results = static_cast<ObJsonArray *>(response);
  }
  return ret;
}

int ObExprAIRerank::append_rerank_batch(
    ObJsonArray *batch, int64_t start, int64_t count, ObArray<RerankResult> &results)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> indices;
  if (OB_ISNULL(batch) || count <= 0 || batch->element_count() != count
      || start < 0 || start > INT64_MAX - count) {
    ret = OB_INVALID_DATA;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObJsonNode *node = batch->get_value(i);
      ObJsonNode *index_node = nullptr;
      ObJsonNode *score_node = nullptr;
      ObJsonObject *object = nullptr;
      int64_t index = -1;
      double score = 0;
      if (OB_ISNULL(node) || node->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_DATA;
      } else {
        object = static_cast<ObJsonObject *>(node);
        index_node = object->get_value(INDEX_KEY);
        score_node = object->get_value(SCORE_KEY);
        if (OB_ISNULL(index_node) || OB_ISNULL(score_node)) {
          ret = OB_INVALID_DATA;
        } else if (index_node->json_type() == ObJsonNodeType::J_INT) {
          index = index_node->get_int();
        } else if (index_node->json_type() == ObJsonNodeType::J_UINT
                   && index_node->get_uint() < static_cast<uint64_t>(count)) {
          index = static_cast<int64_t>(index_node->get_uint());
        } else {
          ret = OB_INVALID_DATA;
        }
      }
      if (OB_SUCC(ret)) {
        if (index < 0 || index >= count) {
          ret = OB_INVALID_DATA;
        } else if (score_node->json_type() != ObJsonNodeType::J_INT
                   && score_node->json_type() != ObJsonNodeType::J_UINT
                   && score_node->json_type() != ObJsonNodeType::J_DOUBLE
                   && score_node->json_type() != ObJsonNodeType::J_DECIMAL) {
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(score_node->to_double(score))) {
        } else if (!std::isfinite(score)) {
          ret = OB_INVALID_DATA;
        }
      }
      for (int64_t seen = 0; OB_SUCC(ret) && seen < indices.count(); ++seen) {
        if (indices.at(seen) == index) {
          ret = OB_INVALID_DATA;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(indices.push_back(index))) {
      } else if (OB_FAIL(results.push_back(RerankResult(start + index, object)))) {
      }
    }
  }
  if (ret == OB_INVALID_DATA) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, invalid result indices, scores or result count");
  }
  return ret;
}
int ObExprAIRerank::sort_rerank_results(ObArray<RerankResult> &results)
{
  int ret = OB_SUCCESS;
  const int64_t count = results.count();
  ObArray<RerankResult> scratch;
  if (count > 1 && OB_FAIL(scratch.assign(results))) {
  } else {
    // Double conversion would collapse distinct large integer scores.
    for (int64_t width = 1; OB_SUCC(ret) && width < count;
         width = width > count / 2 ? count : width * 2) {
      for (int64_t start = 0; OB_SUCC(ret) && start < count;) {
        const int64_t middle = start + MIN(width, count - start);
        const int64_t end = middle + MIN(width, count - middle);
        int64_t left = start;
        int64_t right = middle;
        for (int64_t out = start; OB_SUCC(ret) && out < end; ++out) {
          bool take_left = right == end;
          if (left == middle) {
            take_left = false;
          } else if (right < end) {
            const RerankResult &left_item = results.at(left);
            const RerankResult &right_item = results.at(right);
            const ObJsonNode *left_score = left_item.object_->get_value(SCORE_KEY);
            const ObJsonNode *right_score = right_item.object_->get_value(SCORE_KEY);
            int comparison = 0;
            if (OB_ISNULL(left_score) || OB_ISNULL(right_score)) {
              ret = OB_INVALID_DATA;
            } else if (OB_FAIL(left_score->compare(*right_score, comparison))) {
            } else {
              take_left = comparison > 0
                  || (comparison == 0 && left_item.index_ < right_item.index_);
            }
          }
          if (OB_SUCC(ret)) {
            scratch.at(out) = results.at(take_left ? left++ : right++);
          }
        }
        start = end;
      }
      if (OB_SUCC(ret) && OB_FAIL(results.assign(scratch))) {
      }
    }
  }
  return ret;
}

int ObExprAIRerank::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const 
{
  int ret = OB_SUCCESS;
  // TODO: support schema version match in plan cache for ai func
  // const ObRawExpr *model_key = raw_expr.get_param_expr(0);
  // if (OB_NOT_NULL(model_key)
  //     && (model_key->is_static_scalar_const_expr() || model_key->is_const_expr())
  //     && model_key->get_expr_type() != T_OP_GET_USER_VAR &&
  //     OB_NOT_NULL(expr_cg_ctx.schema_guard_)) {
  //   ObIAllocator *allocator = expr_cg_ctx.allocator_;
  //   ObExecContext *exec_ctx = expr_cg_ctx.session_->get_cur_exec_ctx();
  //   bool got_data = false;
  //   ObObj const_data;
  //   ObAIFuncExprInfo *info = nullptr;
  //   if (OB_ISNULL(allocator)) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_WARN("allocator is null", K(ret));
  //   } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
  //                                                         model_key,
  //                                                         const_data,
  //                                                         got_data,
  //                                                         *allocator))) {
  //     LOG_WARN("failed to calc offset expr", K(ret));
  //   } else if (!got_data || const_data.is_null()) {
  //   } else {
  //     ObAIFuncExprInfo *info = OB_NEWx(ObAIFuncExprInfo, allocator, *allocator, T_FUN_SYS_AI_RERANK);
  //     if (OB_ISNULL(info)) {
  //       ret = OB_ALLOCATE_MEMORY_FAILED;
  //       LOG_WARN("failed to allocate memory for ai rerank info", K(ret));
  //     } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(*allocator, const_data.get_string(), *expr_cg_ctx.schema_guard_, info))) {
  //       LOG_WARN("failed to get ai func info", K(ret), K(const_data.get_string()));
  //     } else {
  //       rt_expr.extra_info_ = info;
  //     }
  //   }
  // }
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIRerank::eval_ai_rerank;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
