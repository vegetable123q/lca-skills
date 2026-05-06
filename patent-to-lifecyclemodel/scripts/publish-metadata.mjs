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

function processInstances(payload) {
  const model = root(payload);
  const tech = model.lifeCycleModelInformation?.technology || {};
  return listify(tech.processes?.processInstance).filter(isRecord);
}

function processLabel(reference, fallback) {
  return text(reference.name) || text(reference['common:shortDescription']) || fallback;
}

function matchingResultingProcessType(instance, payload) {
  const model = root(payload);
  const quantitativeReference =
    model.lifeCycleModelInformation?.quantitativeReference || {};
  const referenceProcessInternalId = text(
    quantitativeReference.referenceToReferenceProcess,
  );
  const instanceInternalId = text(instance['@dataSetInternalID']);
  if (referenceProcessInternalId && instanceInternalId === referenceProcessInternalId) {
    return 'primary';
  }

  const reference = isRecord(instance.referenceToProcess) ? instance.referenceToProcess : {};
  const resulting =
    model.lifeCycleModelInformation?.dataSetInformation?.referenceToResultingProcess || {};
  const processId = text(reference['@refObjectId']);
  const processVersion = text(reference['@version']);
  const resultingId = text(resulting['@refObjectId']);
  const resultingVersion = text(resulting['@version']);
  return processId && processId === resultingId && (!resultingVersion || processVersion === resultingVersion)
    ? 'primary'
    : 'secondary';
}

export function buildPatentLifecyclemodelJsonTg(payload) {
  const instances = processInstances(payload);
  const columnCount = Math.max(1, Math.ceil(Math.sqrt(Math.max(instances.length, 1))));
  const nodes = instances.map((instance, index) => {
    const reference = isRecord(instance.referenceToProcess) ? instance.referenceToProcess : {};
    const internalId = text(instance['@dataSetInternalID']) || `node-${index + 1}`;
    const processId = text(reference['@refObjectId']) || `process-${index + 1}`;
    const version = text(reference['@version']);
    const label = processLabel(reference, processId);
    return {
      id: internalId,
      x: (index % columnCount) * 420,
      y: Math.floor(index / columnCount) * 180,
      width: 350,
      height: 120,
      data: {
        id: processId,
        ...(version ? { version } : {}),
        label,
        name: label,
        shortDescription: label,
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
        edges.push({
          id: [sourceCell, targetCell, flowUuid || `edge-${edges.length + 1}`].join(':'),
          source: { cell: sourceCell },
          target: { cell: targetCell },
          labels: [],
          data: {
            connection: {
              outputExchange: {
                ...(flowUuid ? { '@flowUUID': flowUuid } : {}),
                downstreamProcess: clone(downstreamProcess),
              },
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
  let changed = false;

  for (const entry of listify(bundle.lifecyclemodels).filter(isRecord)) {
    const payload = loadLifecyclemodelPayload(entry, bundleDir);
    if (!payload) continue;
    delete entry.file;
    delete entry.path;
    entry.json_ordered = payload;
    entry.json_tg = buildPatentLifecyclemodelJsonTg(payload);
    changed = true;
  }

  if (changed) {
    fs.writeFileSync(resolved, `${JSON.stringify(bundle, null, 2)}\n`);
  }
  return { bundlePath: resolved, changed };
}
