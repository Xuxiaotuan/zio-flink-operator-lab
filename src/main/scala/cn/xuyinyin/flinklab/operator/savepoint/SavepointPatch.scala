package cn.xuyinyin.flinklab.operator.savepoint

object SavepointPatch:
  // Keep the Long literal intact: ujson.Num is backed by Double.
  def trigger(nonce: Long): String =
    s"""{"spec":{"job":{"savepointTriggerNonce":$nonce}}}"""

  // Let the Operator perform stop-with-savepoint as one reconciliation flow.
  def suspend: String =
    """{"spec":{"job":{"state":"suspended","upgradeMode":"savepoint"}}}"""
