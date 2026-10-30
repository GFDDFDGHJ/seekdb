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
#include "ob_expr_ai_embed.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase 
{
namespace sql 
{
ObExprAIEmbed::ObExprAIEmbed(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, 
                        T_FUN_SYS_AI_EMBED, 
                        N_AI_EMBED, 
                        MORE_THAN_ZERO,
                        NOT_VALID_FOR_GENERATED_COL, 
                        NOT_ROW_DIMENSION) 
{
}

ObExprAIEmbed::~ObExprAIEmbed() 
{
}

int ObExprAIEmbed::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types_stack,
                                     int64_t param_num,
                                     common::ObExprTypeCtx &type_ctx) const 
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3 || param_num < 2)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
    types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (!ob_is_text_tc(types_stack[CONTENT_IDX].get_type())) {
      types_stack[CONTENT_IDX].set_calc_type(ObVarcharType);
    }
    types_stack[CONTENT_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (param_num == 3) {
      const ObObjType dim_type = types_stack[DIM_IDX].get_type();
      if (ob_is_integer_type(dim_type) || dim_type == ObNullType) {
        types_stack[DIM_IDX].set_calc_type(ob_is_unsigned_type(dim_type) ? ObUInt64Type : ObIntType);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be an integer, not a decimal or float");
      }
    }
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) 
{
  INIT_SUCC(ret);
  ObDatum *arg_model_id = nullptr;
  ObDatum *arg_content = nullptr;
  ObDatum *arg_dim = nullptr;
  if (expr.arg_cnt_ == 3 ? OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content, arg_dim))
                         : OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content))) {
  } else if (arg_model_id->is_null() || arg_content->is_null()) {
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_EMBED));
    ObString model_id = arg_model_id->get_string();
    ObString content;
    int64_t dimension = 0;
    ObArray<ObString> contents;
    ObArray<ObString> results;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(
          ctx.exec_ctx_, temp_allocator, *arg_content, expr.args_[CONTENT_IDX]->datum_meta_,
          expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(), content))) {
    } else if (model_id.empty() || content.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id or input is empty");
    } else if (OB_FAIL(get_dimension(expr, arg_dim, dimension))) {
    } else if (OB_FAIL(contents.push_back(content))) {
    } else if (OB_FAIL(call_embeddings(temp_allocator, model_id, dimension, contents, results))) {
    } else if (results.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, results.at(0)))) {
    }
  }
  return ret;
}

int ObExprAIEmbed::get_dimension(const ObExpr &expr, const ObDatum *datum, int64_t &dimension)
{
  int ret = OB_SUCCESS;
  dimension = 0;
  if (OB_NOT_NULL(datum)) {
    if (datum->is_null()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (ob_is_unsigned_type(expr.args_[DIM_IDX]->datum_meta_.type_)) {
      const uint64_t value = datum->get_uint();
      if (value > static_cast<uint64_t>(INT64_MAX)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        dimension = static_cast<int64_t>(value);
      }
    } else {
      dimension = datum->get_int();
    }
    if (OB_SUCC(ret) && dimension <= 0) {
      ret = OB_INVALID_ARGUMENT;
    }
    if (OB_FAIL(ret)) {
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be a positive integer");
    }
  }
  return ret;
}

int ObExprAIEmbed::call_embeddings(ObIAllocator &allocator, ObString &model_id,
                                  const int64_t dimension, ObArray<ObString> &contents,
                                  ObArray<ObString> &results)
{
  int ret = OB_SUCCESS;
  ObAIFuncExprInfo *info = nullptr;
  share::ObAiModelEndpointInfo endpoint;
  query::ObIAiEndpointResolver *resolver =
      ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
  ObJsonObject *config = nullptr;
  ObJsonInt *dim_json = nullptr;
  if (dimension > 0) {
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, dimension, dim_json))) {
    } else if (OB_FAIL(config->add("dimensions", dim_json))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, model_id, info))) {
  } else if (OB_ISNULL(info) || OB_ISNULL(resolver)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(resolver->resolve_by_model_name(model_id, allocator, endpoint))) {
  } else {
    ObAIFuncModel model(allocator, *info, endpoint);
    if (OB_FAIL(model.call_dense_embedding_vector_v2(contents, config, results))) {
    } else if (results.count() != contents.count()) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                     const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (batch_size <= 0) {
    // No active rows.
  } else {
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    MultimodeAlloctor allocator(alloc_guard.get_allocator());
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_EMBED));
    ObEvalCtx::BatchInfoScopeGuard batch_guard(ctx);
    batch_guard.set_batch_size(batch_size);
    ObDatum *results = expr.locate_batch_datums(ctx);
    ObBitVector &evaluated = expr.get_evaluated_flags(ctx);
    ObBitVector *active_skip = nullptr;
    void *skip_buf = allocator.alloc(ObBitVector::memory_size(batch_size));
    if (OB_ISNULL(results)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ISNULL(skip_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      active_skip = to_bit_vector(skip_buf);
      active_skip->bit_calculate(skip, evaluated, batch_size,
          [](uint64_t left, uint64_t right) { return left | right; });
      for (int64_t arg = 0; OB_SUCC(ret) && !active_skip->is_all_true(batch_size)
                           && arg < expr.arg_cnt_; ++arg) {
        if (OB_FAIL(expr.args_[arg]->eval_batch(ctx, *active_skip, batch_size))) {
        }
      }
    }

    ObArray<int64_t> rows;
    ObArray<int64_t> dimensions;
    ObArray<ObString> models;
    ObArray<ObString> contents;
    for (int64_t row = 0; OB_SUCC(ret) && row < batch_size; ++row) {
      if (active_skip->at(row)) {
        continue;
      }
      const ObDatum &model_datum = expr.args_[MODEL_IDX]->locate_expr_datum(ctx, row);
      const ObDatum &content_datum = expr.args_[CONTENT_IDX]->locate_expr_datum(ctx, row);
      if (model_datum.is_null() || content_datum.is_null()) {
        results[row].set_null();
        evaluated.set(row);
        expr.get_eval_info(ctx).notnull_ = false;
      } else {
        ObString model_id = model_datum.get_string();
        ObString content;
        int64_t dimension = 0;
        const ObDatum *dim = expr.arg_cnt_ == 3
            ? &expr.args_[DIM_IDX]->locate_expr_datum(ctx, row) : nullptr;
        batch_guard.set_batch_idx(row);
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(
              ctx.exec_ctx_, allocator, content_datum, expr.args_[CONTENT_IDX]->datum_meta_,
              expr.args_[CONTENT_IDX]->obj_meta_.has_lob_header(), content))) {
        } else if (model_id.empty() || content.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, model id or input is empty");
        } else if (OB_FAIL(get_dimension(expr, dim, dimension))) {
        } else if (OB_FAIL(rows.push_back(row))) {
        } else if (OB_FAIL(models.push_back(model_id))) {
        } else if (OB_FAIL(dimensions.push_back(dimension))) {
        } else if (OB_FAIL(contents.push_back(content))) {
        }
      }
    }

    // Batch only compatible arguments together, retaining the original frame offsets.
    for (int64_t first = 0; OB_SUCC(ret) && first < rows.count(); ++first) {
      if (evaluated.at(rows.at(first))) {
        continue;
      }
      ObArray<ObString> group_contents;
      ObArray<int64_t> group_rows;
      ObArray<ObString> group_results;
      for (int64_t i = first; OB_SUCC(ret) && i < rows.count(); ++i) {
        if (!evaluated.at(rows.at(i)) && models.at(i) == models.at(first)
            && dimensions.at(i) == dimensions.at(first)) {
          if (OB_FAIL(group_contents.push_back(contents.at(i)))) {
          } else if (OB_FAIL(group_rows.push_back(rows.at(i)))) {
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(call_embeddings(allocator, models.at(first), dimensions.at(first),
                                       group_contents, group_results))) {
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < group_rows.count(); ++i) {
          const int64_t row = group_rows.at(i);
          batch_guard.set_batch_idx(row);
          if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, results[row], group_results.at(i)))) {
          } else {
            evaluated.set(row);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::cg_expr(ObExprCGCtx &expr_cg_ctx, 
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
  //   ObObj const_data;
  //   bool got_data = false;
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
  //   } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(*allocator, const_data.get_string(), *expr_cg_ctx.schema_guard_, info))) {
  //     LOG_WARN("failed to get ai func info", K(ret), K(const_data.get_string()));
  //   } else {
  //     rt_expr.extra_info_ = info;
  //   }
  // }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIEmbed::eval_ai_embed;
    rt_expr.eval_batch_func_ = ObExprAIEmbed::eval_ai_embed_batch;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
