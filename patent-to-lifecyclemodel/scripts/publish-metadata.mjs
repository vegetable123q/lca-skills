import fs from 'node:fs';
import path from 'node:path';

function isRecord(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function listify(value) {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function text(value) {
  if (typeof value === 'string') return value.trim();
  if (Array.isArray(value)) {
    for (const item of value) {
      const found = text(item);
      if (found) return found;
    }
    return '';
  }
  if (isRecord(value)) {
    if (typeof value['#text'] === 'string') return value['#text'].trim();
    if (isRecord(value.baseName) || Array.isArray(value.baseName)) {
      return text(value.baseName);
    }
    for (const nested of Object.values(value)) {
      const found = text(nested);
      if (found) return found;
    }
  }
  return '';
}

function root(payload) {
  return isRecord(payload.lifeCycleModelDataSet) ? payload.lifeCycleModelDataSet : payload;
}

function compactParts(parts) {
  return parts.filter((part) => typeof part === 'string' && part.trim()).map((part) => part.trim());
}

function patentSourceSummary(source) {
  if (!isRecord(source)) return '';
  return compactParts([
    source.source_id ? `patent ${source.source_id}` : '',
    source.assignee ? `assignee ${source.assignee}` : '',
    source.year ? `year ${source.year}` : '',
    source.priority_date ? `priority ${source.priority_date}` : '',
    source.publication_date ? `publication ${source.publication_date}` : '',
    source.grant_date ? `grant ${source.grant_date}` : '',
    source.inventor ? `inventor ${source.inventor}` : '',
  ]).join('; ');
}

function patentSourceComment(manifest) {
  const source = manifest?.basic_info?.source;
  const summary = patentSourceSummary(source);
  if (!summary) return null;
  return {
    '@xml:lang': 'en',
    '#text': `Patent source metadata: ${summary}.`,
  };
}

function appendUniqueGeneralComment(target, comment) {
  if (!comment) return false;
  const existing = listify(target['common:generalComment']).filter(isRecord);
  const commentText = text(comment);
  const withoutPriorPatentMetadata = existing.filter(
    (entry) => !text(entry).startsWith('Patent source metadata:'),
  );
  if (withoutPriorPatentMetadata.some((entry) => text(entry) === commentText)) {
    target['common:generalComment'] = withoutPriorPatentMetadata;
    return false;
  }
  target['common:generalComment'] = [...withoutPriorPatentMetadata, comment];
  return true;
}

function applyPatentBasicInfoToLifecyclemodelPayload(payload, manifest) {
  if (!isRecord(manifest?.basic_info)) return false;
  const model = root(payload);
  const info = model.lifeCycleModelInformation || {};
  model.lifeCycleModelInformation = info;
  const dataSetInformation = info.dataSetInformation || {};
  info.dataSetInformation = dataSetInformation;
  let changed = appendUniqueGeneralComment(dataSetInformation, patentSourceComment(manifest));
  if (manifest.basic_info.reference_year && !text(dataSetInformation.referenceYear)) {
    dataSetInformation.referenceYear = String(manifest.basic_info.reference_year);
    changed = true;
  }
  dataSetInformation.patentSource = clone(manifest.basic_info.source || {});
  return true;
}

function processInstances(payload) {
  const model = root(payload);
  const tech = model.lifeCycleModelInformation?.technology || {};
  return listify(tech.processes?.processInstance).filter(isRecord);
}

function processLabel(reference, fallback) {
  return text(reference.name) || text(reference['common:shortDescription']) || fallback;
}

function localizedDescription(reference, fallback) {
  const shortDescription = reference['common:shortDescription'];
  if (shortDescription !== undefined) {
    const cloned = clone(shortDescription);
    return Array.isArray(cloned) ? cloned : [cloned];
  }
  const name = reference.name;
  if (name !== undefined) {
    const cloned = clone(name);
    return Array.isArray(cloned) ? cloned : [cloned];
  }
  return [{ '@xml:lang': 'en', '#text': fallback }];
}

function localizedName(reference, fallback) {
  if (isRecord(reference.name)) {
    return clone(reference.name);
  }
  return { baseName: localizedDescription(reference, fallback) };
}

function referenceProcessInternalId(payload) {
  const model = root(payload);
  return text(
    model.lifeCycleModelInformation?.quantitativeReference?.referenceToReferenceProcess,
  );
}

function matchingResultingProcessType(instance, payload) {
  const model = root(payload);
  const referenceProcessInternalIdValue = referenceProcessInternalId(payload);
  const instanceInternalId = text(instance['@dataSetInternalID']);
  if (referenceProcessInternalIdValue && instanceInternalId === referenceProcessInternalIdValue) {
    return 'primary';
  }

  const reference = isRecord(instance.referenceToProcess) ? instance.referenceToProcess : {};
  const resulting =
    model.lifeCycleModelInformation?.dataSetInformation?.referenceToResultingProcess || {};
  const processId = text(reference['@refObjectId']);
  const processVersion = text(reference['@version']);
  const resultingId = text(resulting['@refObjectId']);
  const resultingVersion = text(resulting['@version']);
  return processId &&
    processId === resultingId &&
    (!resultingVersion || processVersion === resultingVersion)
    ? 'primary'
    : 'secondary';
}

export function buildPatentLifecyclemodelJsonTg(payload, options = {}) {
  return buildPatentLifecyclemodelJsonTgWithOptions(payload, options);
}

function processDatasetRoot(payload) {
  return isRecord(payload?.processDataSet) ? payload.processDataSet : payload;
}

function processIdFromPayload(payload) {
  const rootPayload = processDatasetRoot(payload);
  return text(rootPayload?.processInformation?.dataSetInformation?.['common:UUID']);
}

function exchangeFlowId(exchange) {
  return text(exchange?.referenceToFlowDataSet?.['@refObjectId']);
}

function exchangeLabel(exchange, fallback) {
  return text(exchange?.referenceToFlowDataSet?.['common:shortDescription']) || fallback;
}

function exchangeShortDescription(exchange, fallback) {
  const description = exchange?.referenceToFlowDataSet?.['common:shortDescription'];
  if (description !== undefined) {
    const cloned = clone(description);
    return Array.isArray(cloned) ? cloned : [cloned];
  }
  return [{ '@xml:lang': 'en', '#text': fallback }];
}

function buildProcessExchangeIndex(processPayloads = []) {
  const byProcessId = new Map();
  for (const payload of processPayloads.filter(isRecord)) {
    const processId = processIdFromPayload(payload);
    if (!processId) continue;
    const rootPayload = processDatasetRoot(payload);
    const exchanges = listify(rootPayload?.exchanges?.exchange).filter(isRecord);
    const summary = {
      byDirectionAndFlow: new Map(),
      exchanges: [],
      exchangeCount: exchanges.length,
    };
    for (const exchange of exchanges) {
      const flowId = exchangeFlowId(exchange);
      const direction = text(exchange.exchangeDirection);
      if (!flowId || !direction) continue;
      const flowName = exchangeLabel(exchange, flowId);
      const shortDescription = exchangeShortDescription(exchange, flowName);
      const flowVersion = text(exchange.referenceToFlowDataSet?.['@version']);
      const exchangeSummary = {
        exchangeInternalId: text(exchange['@dataSetInternalID']),
        flowId,
        direction,
        name: flowName,
        shortDescription,
        referenceUnit: text(exchange.referenceUnit),
        meanAmount: exchange.meanAmount,
        flowVersion,
        allocations: clone(exchange.allocations || { allocation: {} }),
        quantitativeReference:
          exchange.quantitativeReference === true || text(exchange.quantitativeReference) === '1',
      };
      summary.exchanges.push(exchangeSummary);
    }
    const portCounts = new Map();
    for (const exchange of summary.exchanges) {
      const directionCode = exchange.direction.toUpperCase();
      const countKey = `${directionCode}\u0000${exchange.flowId}`;
      const count = portCounts.get(countKey) || 0;
      portCounts.set(countKey, count + 1);
      exchange.portId =
        count === 0
          ? `${directionCode}:${exchange.flowId}`
          : `${directionCode}:${exchange.flowId}:${exchange.exchangeInternalId || count + 1}`;
    }
    for (const exchange of summary.exchanges) {
      const key = `${exchange.direction}\u0000${exchange.flowId}`;
      const current = summary.byDirectionAndFlow.get(key);
      if (!current || (!current.quantitativeReference && exchange.quantitativeReference)) {
        summary.byDirectionAndFlow.set(key, exchange);
      }
    }
    const current = byProcessId.get(processId);
    if (!current || summary.exchangeCount >= current.exchangeCount) {
      byProcessId.set(processId, summary);
    }
  }
  return byProcessId;
}

function findExchangeSummary(index, processId, direction, flowId) {
  return index.get(processId)?.byDirectionAndFlow.get(`${direction}\u0000${flowId}`) || null;
}

function portGroups() {
  return {
    groupInput: {
      attrs: {
        text: { fill: 'rgba(0,0,0,0.45)', fontSize: 14 },
        portBody: {
          x: -4,
          y: -4,
          fill: '#fff',
          width: 8,
          height: 8,
          magnet: true,
          stroke: '#5c246a',
          strokeWidth: 1,
        },
      },
      label: { position: { name: 'right' } },
      markup: [{ tagName: 'rect', selector: 'portBody' }],
      position: { name: 'absolute' },
    },
    groupOutput: {
      attrs: {
        text: { fill: 'rgba(0,0,0,0.45)', fontSize: 14 },
        portBody: {
          x: -4,
          y: -4,
          fill: '#fff',
          width: 8,
          height: 8,
          magnet: true,
          stroke: '#5c246a',
          strokeWidth: 1,
        },
      },
      label: { position: { name: 'left' } },
      markup: [{ tagName: 'rect', selector: 'portBody' }],
      position: { name: 'absolute' },
    },
  };
}

function truncateForGraph(value, maxLength = 28) {
  const normalized = text(value);
  return normalized.length > maxLength ? `${normalized.slice(0, maxLength)}...` : normalized;
}

function buildPort(exchange, indexByDirection) {
  const directionCode = exchange.direction.toUpperCase();
  const isInput = directionCode === 'INPUT';
  const y = 65 + indexByDirection * 20;
  const title = exchange.name;
  const allocationFraction = text(exchange.allocations?.allocation?.['@allocatedFraction']);
  const prefix = allocationFraction && !isInput ? `[${allocationFraction}] ` : '';
  const display = `${prefix}${title}`;
  return {
    id: exchange.portId || `${directionCode}:${exchange.flowId}`,
    args: { x: isInput ? 0 : '100%', y },
    data: {
      flowId: exchange.flowId,
      textLang: clone(exchange.shortDescription),
      ...(exchange.flowVersion ? { flowVersion: exchange.flowVersion } : {}),
      allocations: clone(exchange.allocations),
      quantitativeReference: exchange.quantitativeReference,
    },
    attrs: {
      text: {
        fill:
          exchange.quantitativeReference ||
          (allocationFraction && Number.parseFloat(allocationFraction) > 0)
            ? '#5c246a'
            : 'rgba(0,0,0,0.45)',
        text: truncateForGraph(display, 28),
        title: display,
        cursor: 'pointer',
        'font-weight': exchange.quantitativeReference ? 'bold' : 'normal',
      },
    },
    group: isInput ? 'groupInput' : 'groupOutput',
    tools: [{ id: 'portTool' }],
  };
}

function buildPorts(processExchangeIndex, processId) {
  const summary = processExchangeIndex.get(processId);
  const exchanges = summary?.exchanges || [];
  const directionCounts = new Map();
  const items = exchanges.map((exchange) => {
    const directionCode = exchange.direction.toUpperCase();
    const index = directionCounts.get(directionCode) || 0;
    directionCounts.set(directionCode, index + 1);
    return buildPort(exchange, index);
  });
  return { items, groups: portGroups() };
}

function buildPatentLifecyclemodelJsonTgWithOptions(payload, options = {}) {
  const instances = processInstances(payload);
  const processExchangeIndex = buildProcessExchangeIndex(options.processPayloads || []);
  const referenceProcessInternalIdValue = referenceProcessInternalId(payload);
  const columnCount = Math.max(1, Math.ceil(Math.sqrt(Math.max(instances.length, 1))));
  const nodes = instances.map((instance, index) => {
    const reference = isRecord(instance.referenceToProcess) ? instance.referenceToProcess : {};
    const internalId = text(instance['@dataSetInternalID']) || `node-${index + 1}`;
    const processId = text(reference['@refObjectId']) || `process-${index + 1}`;
    const version = text(reference['@version']);
    const label = processLabel(reference, processId);
    const ports = buildPorts(processExchangeIndex, processId);
    const sidePortCount = Math.max(
      ports.items.filter((port) => port.group === 'groupInput').length,
      ports.items.filter((port) => port.group === 'groupOutput').length,
    );
    const height = Math.max(100, 55 + sidePortCount * 20);
    const isReferenceProcess =
      referenceProcessInternalIdValue && internalId === referenceProcessInternalIdValue;
    return {
      id: internalId,
      x: (index % columnCount) * 420,
      y: Math.floor(index / columnCount) * 180,
      width: 350,
      height,
      size: { width: 350, height },
      shape: 'rect',
      ports,
      data: {
        id: processId,
        index: String(index),
        ...(version ? { version } : {}),
        label: localizedName(reference, label),
        labelText: label,
        name: label,
        nameText: label,
        shortDescription: localizedDescription(reference, label),
        displayName: localizedDescription(reference, label),
        quantitativeReference: isReferenceProcess ? '1' : '0',
      },
    };
  });

  const nodeByInternalId = Object.fromEntries(
    nodes.map((node) => [
      node.id,
      {
        processId: node.data.id,
        version: node.data.version || null,
      },
    ]),
  );
  const edges = [];
  instances.forEach((instance, index) => {
    const sourceCell = text(instance['@dataSetInternalID']) || `node-${index + 1}`;
    const sourceNode = nodeByInternalId[sourceCell] || {};
    for (const outputExchange of listify(instance.connections?.outputExchange).filter(isRecord)) {
      const flowUuid = text(outputExchange['@flowUUID']);
      for (const downstreamProcess of listify(outputExchange.downstreamProcess).filter(isRecord)) {
        const targetCell = text(downstreamProcess['@id']);
        if (!targetCell) continue;
        const targetNode = nodeByInternalId[targetCell] || {};
        const outputFlow = findExchangeSummary(
          processExchangeIndex,
          sourceNode.processId,
          'Output',
          flowUuid,
        );
        const inputFlow = findExchangeSummary(
          processExchangeIndex,
          targetNode.processId,
          'Input',
          text(downstreamProcess['@flowUUID']) || flowUuid,
        );
        const flowLabel = outputFlow?.name || inputFlow?.name || flowUuid;
        edges.push({
          id: [sourceCell, targetCell, flowUuid || `edge-${edges.length + 1}`].join(':'),
          source: {
            cell: sourceCell,
            ...(outputFlow?.portId
              ? { port: outputFlow.portId }
              : flowUuid
                ? { port: `OUTPUT:${flowUuid}` }
                : {}),
          },
          target: {
            cell: targetCell,
            ...(inputFlow?.portId
              ? { port: inputFlow.portId }
              : flowUuid
                ? { port: `INPUT:${text(downstreamProcess['@flowUUID']) || flowUuid}` }
                : {}),
          },
          shape: 'edge',
          attrs: { line: { stroke: '#5c246a', strokeWidth: 1 } },
          labels: flowLabel ? [{ text: flowLabel }] : [],
          data: {
            connection: {
              outputExchange: {
                ...(flowUuid ? { '@flowUUID': flowUuid } : {}),
                ...(outputFlow?.shortDescription?.length
                  ? { 'common:shortDescription': outputFlow.shortDescription }
                  : {}),
                downstreamProcess: clone(downstreamProcess),
              },
            },
            flow: {
              id: flowUuid || inputFlow?.flowId || outputFlow?.flowId,
              name: flowLabel,
              output: outputFlow
                ? {
                    processId: sourceNode.processId,
                    exchangeInternalId: outputFlow.exchangeInternalId,
                    flowId: outputFlow.flowId,
                    name: outputFlow.name,
                    referenceUnit: outputFlow.referenceUnit,
                    meanAmount: outputFlow.meanAmount,
                  }
                : null,
              input: inputFlow
                ? {
                    processId: targetNode.processId,
                    exchangeInternalId: inputFlow.exchangeInternalId,
                    flowId: inputFlow.flowId,
                    name: inputFlow.name,
                    referenceUnit: inputFlow.referenceUnit,
                    meanAmount: inputFlow.meanAmount,
                  }
                : null,
            },
            node: {
              sourceNodeID: sourceCell,
              targetNodeID: targetCell,
              sourceProcessId: sourceNode.processId,
              ...(sourceNode.version ? { sourceProcessVersion: sourceNode.version } : {}),
              ...(targetNode.processId ? { targetProcessId: targetNode.processId } : {}),
              ...(targetNode.version ? { targetProcessVersion: targetNode.version } : {}),
            },
          },
        });
      }
    }
  });

  const submodels = instances
    .map((instance, index) => {
      const reference = isRecord(instance.referenceToProcess) ? instance.referenceToProcess : {};
      const id = text(reference['@refObjectId']);
      if (!id) return null;
      const version = text(reference['@version']);
      return {
        id,
        ...(version ? { version } : {}),
        type: matchingResultingProcessType(instance, payload),
        name: processLabel(reference, id),
        instanceId: text(instance['@dataSetInternalID']) || `instance-${index + 1}`,
      };
    })
    .filter(Boolean);

  return {
    xflow: { nodes, edges },
    submodels,
  };
}

export { buildPatentLifecyclemodelJsonTgWithOptions };

function loadLifecyclemodelPayload(entry, bundleDir) {
  if (isRecord(entry.json_ordered)) return entry.json_ordered;
  if (isRecord(entry.jsonOrdered)) return entry.jsonOrdered;
  if (isRecord(entry.json)) return entry.json;
  if (entry.file) {
    const filePath = path.resolve(bundleDir, String(entry.file));
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  }
  return null;
}

export function applyPatentPublishMetadataToBundle(bundlePath) {
  const resolved = path.resolve(bundlePath);
  const bundle = JSON.parse(fs.readFileSync(resolved, 'utf8'));
  const bundleDir = path.dirname(resolved);
  const baseDir = path.dirname(bundleDir);
  const manifestPath = path.join(baseDir, 'manifests', 'lifecyclemodel-manifest.json');
  const manifest = fs.existsSync(manifestPath)
    ? JSON.parse(fs.readFileSync(manifestPath, 'utf8'))
    : null;
  const processPayloads = [];
  const runsDir = path.join(baseDir, 'runs');
  if (fs.existsSync(runsDir)) {
    for (const runName of fs.readdirSync(runsDir).sort()) {
      const exportsDir = path.join(runsDir, runName, 'exports', 'processes');
      if (!fs.existsSync(exportsDir)) continue;
      for (const fileName of fs.readdirSync(exportsDir).sort()) {
        if (!fileName.endsWith('.json')) continue;
        processPayloads.push(JSON.parse(fs.readFileSync(path.join(exportsDir, fileName), 'utf8')));
      }
    }
  }
  let changed = false;

  for (const entry of listify(bundle.lifecyclemodels).filter(isRecord)) {
    const payload = loadLifecyclemodelPayload(entry, bundleDir);
    if (!payload) continue;
    delete entry.file;
    delete entry.path;
    if (manifest?.basic_info) {
      applyPatentBasicInfoToLifecyclemodelPayload(payload, manifest);
      entry.basic_info = clone(manifest.basic_info);
    }
    entry.json_ordered = payload;
    entry.json_tg = buildPatentLifecyclemodelJsonTgWithOptions(payload, { processPayloads });
    changed = true;
  }

  if (changed) {
    fs.writeFileSync(resolved, `${JSON.stringify(bundle, null, 2)}\n`);
  }
  return { bundlePath: resolved, changed };
}
