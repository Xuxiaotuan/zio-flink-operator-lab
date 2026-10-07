package cn.xuyinyin.flinklab.operator.savepoint

/** Savepoint 操作补丁：只生成 Operator 支持的最小 JSON patch，避免误改其它 Job 配置。 */
object SavepointPatch:
  // Keep the Long literal intact: ujson.Num is backed by Double.
  def trigger(nonce: Long): String =
    s"""{"spec":{"job":{"savepointTriggerNonce":$nonce}}}"""

  // Let the Operator perform stop-with-savepoint as one reconciliation flow.
  def suspend: String =
    """{"spec":{"job":{"state":"suspended","upgradeMode":"savepoint"}}}"""
