import test from 'node:test';
import assert from 'node:assert/strict';
import {
  applyFlowResolutionToExchange,
  buildFlowResolution,
} from '../patent-to-lifecyclemodel/scripts/flow-resolution.mjs';

const propertyIds = {
  Mass: '93a60a56-a3c8-11da-a746-0800200b9a66',
  Volume: '93a60a56-a3c8-22da-a746-0800200c9a66',
  'Number of items': '01846770-4cfe-4a25-8ad9-919d8d378345',
};

function flowRow({
  id,
  version,
  name,
  names = [name],
  property = 'Mass',
  typeOfDataSet = 'Product flow',
  classes = [],
  arrayPropertyDescription = false,
}) {
  const shortDescription = arrayPropertyDescription
    ? [{ '@xml:lang': 'en', '#text': property }]
    : { '@xml:lang': 'en', '#text': property };
  return {
    id,
    version,
    json_ordered: {
      flowDataSet: {
        flowInformation: {
          dataSetInformation: {
            'common:UUID': id,
            name: {
              baseName: names.map((entry, index) => ({
                '@xml:lang': index === 0 ? 'zh' : 'en',
                '#text': entry,
              })),
            },
            classificationInformation: {
              'common:classification': {
                'common:class': classes.map((value, index) => ({
                  '@level': String(index),
                  '#text': value,
                })),
              },
            },
          },
        },
        modellingAndValidation: { LCIMethod: { typeOfDataSet } },
        administrativeInformation: {
          publicationAndOwnership: { 'common:dataSetVersion': version },
        },
        flowProperties: {
          flowProperty: {
            '@dataSetInternalID': '0',
            referenceToFlowPropertyDataSet: {
              '@refObjectId': propertyIds[property] || '',
              'common:shortDescription': shortDescription,
            },
          },
        },
      },
    },
  };
}

function uuidsFor(flows) {
  return { flows: Object.fromEntries(Object.keys(flows).map((key) => [key, `new-${key}`])) };
}

function planWith({ flows, inputs = [], outputs = [], processes = null }) {
  return {
    source: { id: 'CN123' },
    flows,
    processes: processes ?? [{ key: 'make_product', inputs, outputs }],
  };
}

function resolve(plan, rows = [], uuids = uuidsFor(plan.flows)) {
  return buildFlowResolution(plan, uuids, rows);
}

test('reuses unique latest exact DB flow and leaves patent-specific flow new', () => {
  const plan = planWith({
    flows: {
      oxygen: { name_en: 'Oxygen', unit: 'kg' },
      product: { name_en: 'Patent-specific cathode matrix', unit: 'kg' },
    },
  });

  const resolution = resolve(plan, [
    flowRow({ id: 'db-oxygen', version: '01.01.000', name: 'oxygen' }),
    flowRow({ id: 'db-oxygen', version: '01.01.002', name: 'Oxygen' }),
  ]);

  assert.equal(resolution.flows.oxygen.id, 'db-oxygen');
  assert.equal(resolution.flows.oxygen.version, '01.01.002');
  assert.equal(resolution.flows.product.decision, 'create_new');
  assert.equal(resolution.summary.reuse_existing, 1);
  assert.equal(resolution.summary.create_new, 1);
});

test('input compatibility rejects non-gas elementary and emission flows', () => {
  const plan = planWith({
    flows: {
      sodium_chloride: { name_en: 'Sodium chloride', unit: 'kg' },
      oxygen: { name_en: 'Oxygen', unit: 'kg' },
    },
    inputs: [{ flow: 'sodium_chloride', amount: 1 }, { flow: 'oxygen', amount: 1 }],
  });

  const resolution = resolve(plan, [
    flowRow({
      id: 'emission-salt',
      version: '03.00.004',
      name: 'Sodium chloride',
      typeOfDataSet: 'Elementary flow',
      classes: ['Emissions', 'Emissions to soil', 'Emissions to non-agricultural soil'],
    }),
    flowRow({ id: 'product-salt', version: '01.00.000', name: 'Sodium chloride' }),
    flowRow({
      id: 'elementary-oxygen',
      version: '03.00.004',
      name: 'oxygen',
      typeOfDataSet: 'Elementary flow',
      classes: ['Natural resources', 'In air'],
    }),
  ]);

  assert.equal(resolution.flows.sodium_chloride.id, 'product-salt');
  assert.equal(resolution.flows.oxygen.id, 'elementary-oxygen');
});

test('emission-only non-gas input stays generated', () => {
  const plan = planWith({
    flows: { sodium_chloride: { name_en: 'Sodium chloride', unit: 'kg' } },
    inputs: [{ flow: 'sodium_chloride', amount: 1 }],
  });

  const resolution = resolve(plan, [
    flowRow({
      id: 'emission-salt',
      version: '03.00.004',
      name: 'Sodium chloride',
      typeOfDataSet: 'Elementary flow',
      classes: ['Emissions'],
    }),
  ]);

  assert.equal(resolution.flows.sodium_chloride.decision, 'create_new');
  assert.equal(resolution.flows.sodium_chloride.id, 'new-sodium_chloride');
});

test('ambiguous exact matches are automatically ranked', () => {
  const plan = planWith({
    flows: { product: { name_en: 'NCM811 cathode material', unit: 'kg' } },
  });

  const resolution = resolve(plan, [
    flowRow({ id: 'db-product-a', version: '01.01.000', name: 'NCM811 cathode material' }),
    flowRow({ id: 'db-product-b', version: '01.01.000', name: 'NCM811 cathode material' }),
  ]);

  assert.equal(resolution.flows.product.decision, 'reuse_existing');
  assert.equal(resolution.flows.product.reason, 'best_exact_name_match');
  assert.equal(resolution.flows.product.id, 'db-product-a');
  assert.equal(resolution.flows.product.candidate_count, 2);
  assert.equal(resolution.review.length, 0);
});

test('common exact matches prefer the latest best candidate', () => {
  const plan = planWith({ flows: { oxygen: { name_en: 'Oxygen', unit: 'kg' } } });
  const resolution = resolve(plan, [
    flowRow({ id: 'db-oxygen-old', version: '01.00.000', name: 'oxygen' }),
    flowRow({ id: 'db-oxygen-new', version: '01.01.000', name: 'Oxygen' }),
  ]);

  assert.equal(resolution.flows.oxygen.id, 'db-oxygen-new');
  assert.equal(resolution.flows.oxygen.reason, 'best_exact_name_match');
  assert.equal(resolution.review.length, 0);
});

test('stable generated UUID and explicit existing refs are honored', () => {
  const stablePlan = planWith({
    flows: { electrolyte: { name_en: 'Patent electrolyte solution', unit: 'kg' } },
  });
  const stable = resolve(
    stablePlan,
    [
      flowRow({ id: 'other-electrolyte', version: '01.00.000', name: 'Patent electrolyte solution' }),
      flowRow({ id: 'stable-electrolyte', version: '01.00.000', name: 'Patent electrolyte solution' }),
    ],
    { flows: { electrolyte: 'stable-electrolyte' } },
  );
  assert.equal(stable.flows.electrolyte.reason, 'stable_uuid_exact_name_match');

  const explicitPlan = planWith({
    flows: {
      electricity: {
        name_en: 'Power use',
        unit: 'MJ',
        existing_flow_ref: {
          id: 'db-electricity-cn',
          version: '02.00.000',
          name: 'Electricity, low voltage',
          unit: 'kWh',
        },
      },
    },
  });
  const explicit = resolve(explicitPlan, []);
  assert.equal(explicit.flows.electricity.reason, 'plan_existing_flow_ref');
  assert.equal(explicit.flows.electricity.id, 'db-electricity-cn');
  assert.equal(explicit.flows.electricity.amount_factor, 0.2777777777777778);
});

test('aliases, multilingual names, energy properties, and normalized names resolve', () => {
  const plan = planWith({
    flows: {
      potassium_chloride: {
        name_en: 'Potassium chloride',
        name_zh: '氯化钾',
        aliases: ['KCl'],
        unit: 'kg',
      },
      electricity: { name_en: 'Power use', aliases: ['Electricity, medium voltage'], unit: 'kWh' },
      manganese_sulfate_tetrahydrate: {
        name_en: 'Manganese sulfate tetrahydrate',
        name_zh: '四水硫酸锰',
        unit: 'kg',
      },
    },
  });

  const resolution = resolve(plan, [
    flowRow({ id: 'db-kcl', version: '01.00.000', name: '氯化钾', names: ['氯化钾', 'Potassium chloride'] }),
    flowRow({
      id: 'db-electricity',
      version: '01.00.000',
      name: 'Electricity, medium voltage',
      property: 'Net calorific value',
      arrayPropertyDescription: true,
    }),
    flowRow({
      id: 'db-manganese-sulfate',
      version: '01.02.000',
      name: '硫酸锰(II)',
      names: ['硫酸锰(II)', 'Manganese(II) sulfate'],
    }),
  ]);

  assert.equal(resolution.flows.potassium_chloride.id, 'db-kcl');
  assert.equal(resolution.flows.electricity.id, 'db-electricity');
  assert.equal(resolution.flows.electricity.unit, 'kWh');
  assert.equal(resolution.flows.manganese_sulfate_tetrahydrate.id, 'db-manganese-sulfate');
  assert.equal(resolution.flows.manganese_sulfate_tetrahydrate.reason, 'best_candidate_name_match');
});

test('nearest matching handles unmatched input materials without reusing product outputs', () => {
  const plan = planWith({
    flows: {
      sodium_chloride: { name_en: 'Sodium chloride molten salt', aliases: ['NaCl'], unit: 'kg' },
      cobalt_metal: { name_en: 'Cobalt metal anode consumed', unit: 'kg' },
      ncm811: { name_en: 'LiNi0.8Co0.1Mn0.1O2 cathode active material, NCM811', unit: 'kg' },
    },
    inputs: [{ flow: 'sodium_chloride', amount: 1 }, { flow: 'cobalt_metal', amount: 1 }],
    outputs: [{ flow: 'ncm811', amount: 1 }],
  });

  const resolution = resolve(plan, [
    flowRow({ id: 'db-sodium-chloride', version: '01.00.000', name: 'sodium chloride' }),
    flowRow({ id: 'db-cobalt', version: '01.00.000', name: 'cobalt' }),
    flowRow({ id: 'db-ncm811', version: '01.00.000', name: 'NCM811' }),
  ]);

  assert.equal(resolution.flows.sodium_chloride.reason, 'nearest_input_name_match');
  assert.equal(resolution.flows.sodium_chloride.id, 'db-sodium-chloride');
  assert.equal(resolution.flows.cobalt_metal.id, 'db-cobalt');
  assert.equal(resolution.flows.ncm811.decision, 'create_new');
});

test('nearest matching rejects simple salt counterion mismatches', () => {
  const plan = planWith({
    flows: {
      sodium_chloride: { name_en: 'Sodium chloride molten salt', aliases: ['NaCl'], unit: 'kg' },
    },
    inputs: [{ flow: 'sodium_chloride', amount: 1 }],
  });

  const resolution = resolve(plan, [
    flowRow({ id: 'db-lithium-chloride', version: '01.01.002', name: 'Lithium chloride' }),
  ]);

  assert.equal(resolution.flows.sodium_chloride.decision, 'create_new');
  assert.equal(resolution.flows.sodium_chloride.id, 'new-sodium_chloride');
});

test('nearest matching keeps patent-specific complex salts from generic compound rows', () => {
  const plan = planWith({
    flows: {
      lithium_zirconium_chloride: {
        name_en: 'Lithium zirconium chloride solid electrolyte',
        aliases: ['Li-Zr-Cl solid electrolyte'],
        unit: 'kg',
      },
    },
    inputs: [{ flow: 'lithium_zirconium_chloride', amount: 1 }],
  });

  const resolution = resolve(plan, [
    flowRow({ id: 'db-zirconium-compound', version: '01.01.001', name: 'Zirconium-based compound' }),
  ]);

  assert.equal(resolution.flows.lithium_zirconium_chloride.decision, 'create_new');
  assert.equal(resolution.flows.lithium_zirconium_chloride.id, 'new-lithium_zirconium_chloride');
});

test('specific metal feedstocks do not collapse to generic Metal or waste-like rows', () => {
  const plan = planWith({
    flows: {
      nickel_metal: { name_en: 'Nickel metal anode consumed', aliases: ['Ni metal'], unit: 'kg' },
      cobalt_metal: { name_en: 'Cobalt metal anode consumed', aliases: ['Co metal'], unit: 'kg' },
      manganese_metal: { name_en: 'Manganese metal anode consumed', aliases: ['Mn metal'], unit: 'kg' },
      ncm811: { name_en: 'LiNi0.8Co0.1Mn0.1O2 cathode active material, NCM811', unit: 'kg' },
    },
    inputs: [
      { flow: 'nickel_metal', amount: 1 },
      { flow: 'cobalt_metal', amount: 1 },
      { flow: 'manganese_metal', amount: 1 },
    ],
    outputs: [{ flow: 'ncm811', amount: 1 }],
  });

  const resolution = resolve(plan, [
    flowRow({ id: 'db-generic-metal', version: '01.00.000', name: 'Metal', property: '' }),
    flowRow({ id: 'db-nickel-metal', version: '01.01.002', name: 'Nickel metal (>99.9% Ni)' }),
    flowRow({ id: 'db-cobalt-electrodeposit', version: '01.01.001', name: 'Electrodeposit Cobalt' }),
    flowRow({
      id: 'db-manganese-slag-remediation',
      version: '01.02.000',
      name: 'manganese slag-based environmental remediation materials',
    }),
    flowRow({ id: 'db-manganese-metallic', version: '01.01.001', name: 'Manganese ore', names: ['Manganese ore', '金属锰'] }),
  ]);

  assert.equal(resolution.flows.nickel_metal.id, 'db-nickel-metal');
  assert.equal(resolution.flows.cobalt_metal.id, 'db-cobalt-electrodeposit');
  assert.equal(resolution.flows.manganese_metal.id, 'db-manganese-metallic');
  for (const key of ['nickel_metal', 'cobalt_metal', 'manganese_metal']) {
    assert.notEqual(resolution.flows[key].id, 'db-generic-metal');
  }
  assert.notEqual(resolution.flows.manganese_metal.id, 'db-manganese-slag-remediation');
});

test('patent-specific internal intermediates stay generated', () => {
  const plan = planWith({
    flows: {
      precursor: { name_en: 'Ni0.8Co0.1Mn0.1(OH)2 precursor', unit: 'kg' },
      lithium_hydroxide: { name_en: 'Lithium hydroxide monohydrate', unit: 'kg' },
    },
    processes: [
      { key: 'make_precursor', inputs: [], outputs: [{ flow: 'precursor', amount: 1 }] },
      {
        key: 'make_product',
        inputs: [{ flow: 'precursor', amount: 1 }, { flow: 'lithium_hydroxide', amount: 1 }],
        outputs: [],
      },
    ],
  });

  const resolution = resolve(plan, [
    flowRow({ id: 'db-precursor', version: '01.00.000', name: 'Ni0.8Co0.1Mn0.1(OH)2 precursor' }),
    flowRow({ id: 'db-lithium-hydroxide', version: '01.00.000', name: 'Lithium hydroxide monohydrate' }),
  ]);

  assert.equal(resolution.flows.precursor.reason, 'patent_internal_intermediate');
  assert.equal(resolution.flows.precursor.id, 'new-precursor');
  assert.equal(resolution.flows.lithium_hydroxide.id, 'db-lithium-hydroxide');
});

test('nearest matching uses compatible specialty reagent families and avoids emissions', () => {
  const specialtyPlan = planWith({
    flows: {
      aluminum_metaphosphate: { name_en: 'Aluminum metaphosphate', aliases: ['Al(PO3)3'], unit: 'kg' },
      lithium_molybdate: { name_en: 'Lithium molybdate', aliases: ['Li2MoO4'], unit: 'kg' },
    },
    inputs: [{ flow: 'aluminum_metaphosphate', amount: 1 }, { flow: 'lithium_molybdate', amount: 1 }],
  });
  const specialty = resolve(specialtyPlan, [
    flowRow({ id: 'db-aluminium-phosphate', version: '01.00.000', name: 'Aluminium Phosphate' }),
    flowRow({ id: 'db-disodium-molybdate', version: '01.00.000', name: 'disodium tetraoxomolybdate dihydrate' }),
  ]);
  assert.equal(specialty.flows.aluminum_metaphosphate.id, 'db-aluminium-phosphate');
  assert.equal(specialty.flows.lithium_molybdate.id, 'db-disodium-molybdate');

  const emissionPlan = planWith({
    flows: { lithium_molybdate: { name_en: 'Lithium molybdate', unit: 'kg' } },
    inputs: [{ flow: 'lithium_molybdate', amount: 1 }],
  });
  const emission = resolve(emissionPlan, [
    flowRow({
      id: 'db-disodium-molybdate-emission',
      version: '00.00.002',
      name: 'disodium tetraoxomolybdate dihydrate',
      typeOfDataSet: 'Elementary flow',
      classes: ['Emissions'],
    }),
    flowRow({ id: 'db-ammonium-orthomolybdate', version: '01.01.001', name: 'Ammonium orthomolybdate' }),
  ]);
  assert.equal(emission.flows.lithium_molybdate.id, 'db-ammonium-orthomolybdate');
});

test('nearest matching rejects incompatible chemical forms and waste-like metal rows', () => {
  const plan = planWith({
    flows: {
      aluminum_nitrate: { name_en: 'Aluminum nitrate', unit: 'kg' },
      niobium_phosphate: {
        name_en: 'Niobium phosphate dopant source',
        aliases: ['niobium phosphorus dopant'],
        unit: 'kg',
      },
      strontium_oxide: { name_en: 'Strontium oxide', unit: 'kg' },
      manganese_rich_shell: { name_en: 'Manganese-rich precursor shell', unit: 'kg' },
      composite_oxide_sol: { name_en: 'Aluminum titanium composite oxide sol', unit: 'kg' },
      tungsten_source: { name_en: 'Tungsten source', unit: 'kg' },
    },
    inputs: [
      { flow: 'aluminum_nitrate', amount: 1 },
      { flow: 'niobium_phosphate', amount: 1 },
      { flow: 'strontium_oxide', amount: 1 },
      { flow: 'manganese_rich_shell', amount: 1 },
      { flow: 'composite_oxide_sol', amount: 1 },
      { flow: 'tungsten_source', amount: 1 },
    ],
  });

  const resolution = resolve(plan, [
    flowRow({ id: 'db-alumina', version: '01.00.000', name: 'Aluminium oxide' }),
    flowRow({ id: 'db-aluminium-primary', version: '01.00.000', name: 'Aluminium, primary, liquid' }),
    flowRow({ id: 'db-niobium-oxide', version: '01.00.000', name: 'Niobium oxide' }),
    flowRow({ id: 'db-calcium-oxide', version: '01.00.000', name: 'Calcium Oxide' }),
    flowRow({ id: 'db-silicon-manganese', version: '01.00.000', name: 'Silicon manganese' }),
    flowRow({
      id: 'db-manganese-remediation',
      version: '01.00.000',
      name: 'manganese slag-based environmental remediation materials',
    }),
    flowRow({ id: 'db-tungsten-filament', version: '01.00.000', name: 'Tungsten filament' }),
    flowRow({ id: 'db-tungsten-ore', version: '01.00.000', name: 'tungsten ore' }),
  ]);

  assert.equal(resolution.flows.aluminum_nitrate.decision, 'create_new');
  assert.equal(resolution.flows.niobium_phosphate.decision, 'create_new');
  assert.equal(resolution.flows.strontium_oxide.decision, 'create_new');
  assert.equal(resolution.flows.manganese_rich_shell.decision, 'create_new');
  assert.equal(resolution.flows.composite_oxide_sol.decision, 'create_new');
  assert.equal(resolution.flows.tungsten_source.decision, 'create_new');
});

test('applyFlowResolutionToExchange writes existing DB flow refs and unit conversions', () => {
  const result = applyFlowResolutionToExchange(
    { flow: 'water', amount: 1000 },
    'Input',
    { flows: { water: { name_en: 'Water', unit: 'g' } } },
    {
      flows: {
        water: {
          decision: 'reuse_existing',
          id: 'db-water',
          version: '03.00.004',
          name: 'water',
          unit: 'kg',
          amount_factor: 0.001,
        },
      },
    },
    { generatedFlowId: 'new-water', exchangeId: '0' },
  );

  assert.equal(result.referenceToFlowDataSet['@refObjectId'], 'db-water');
  assert.equal(result.referenceUnit, 'kg');
  assert.equal(result.meanAmount, 1);
  assert.match(result['common:generalComment'][0]['#text'], /converted from g to kg/u);
});
