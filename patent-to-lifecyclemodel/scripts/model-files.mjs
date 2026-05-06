import fs from 'node:fs';
import path from 'node:path';

export function findBuiltLifecyclemodelFile(lifecyclemodelRunDir) {
  const modelsDir = path.join(lifecyclemodelRunDir, 'models');
  const matches = [];

  if (!fs.existsSync(modelsDir)) {
    throw new Error(`missing built model dir ${modelsDir}; run Stage 5 first`);
  }

  for (const runName of fs.readdirSync(modelsDir).sort()) {
    const candidateDir = path.join(modelsDir, runName, 'tidas_bundle', 'lifecyclemodels');
    if (!fs.existsSync(candidateDir)) continue;
    for (const fileName of fs.readdirSync(candidateDir).sort()) {
      if (fileName.endsWith('.json')) {
        matches.push(path.join(candidateDir, fileName));
      }
    }
  }

  if (matches.length !== 1) {
    throw new Error(
      `expected exactly one built lifecyclemodel under ${modelsDir}, found ${matches.length}`,
    );
  }

  return matches[0];
}
