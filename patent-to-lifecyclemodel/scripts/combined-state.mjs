export function buildCombinedProcessFromFlowState(plan, uuids) {
  const processes = Array.isArray(plan?.processes) ? plan.processes : [];
  const finalProcess = processes[processes.length - 1] || {};
  const refFlowKey = finalProcess.reference_output_flow;
  const refFlow = plan?.flows?.[refFlowKey] || {};
  const flowUuid = uuids?.flows?.[refFlowKey] || null;
  const baseNameEn = refFlow.name_en || refFlow.name || refFlowKey || null;
  const baseNameZh = refFlow.name_zh || null;

  return {
    wrapper: 'patent-to-lifecyclemodel',
    source_id: plan?.source?.id || null,
    flow_summary: {
      wrapper: 'direct',
      uuid: flowUuid,
      version: '01.00.000',
      base_name: baseNameEn,
      base_name_en: baseNameEn,
      base_name_zh: baseNameZh,
      unit: refFlow.unit || plan?.goal?.functional_unit?.unit || 'kg',
      permanent_uri: flowUuid ? `https://local.tiangong.invalid/flows/${flowUuid}` : null,
    },
  };
}
