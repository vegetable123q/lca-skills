#!/usr/bin/env node
// estimate-utilities.mjs
//
// Deterministic utility estimators for LCA plan authoring. Given patent-level
// process conditions (temperature, time, batch mass, reactor type) it returns
// kWh/kg, kg-water/kg, kg-O2/kg, and auxiliary waste/emission factors that are
// directly usable as `Estimated` exchange amounts in plan.json. The same inputs
// across different patents therefore produce comparable LCI values.
//
// Pure stdin/stdout, no external deps, no side effects. LLM runs this and
// copies the result JSON (or its kWh_per_kg / kg_per_kg) into the plan along
// with the formula_ref string, so the number is auditable.
//
// CLI
//   node estimate-utilities.mjs --mode electricity --params '<json>'
//   node estimate-utilities.mjs --mode water       --params '<json>'
//   node estimate-utilities.mjs --mode oxygen      --params '<json>'
//   node estimate-utilities.mjs --mode waste       --params '<json>'
//   node estimate-utilities.mjs --help
//
// --------------------------------------------------------------------------
// FORMULAE (source notes)
//
// 1. Electricity
//    E_total = E_heatup + E_hold + E_stir          [kWh]
//    E_heatup = m_charge × Cp_eff × (T - 25) / 3600
//      m_charge in kg; Cp_eff in kJ·kg^-1·K^-1; T in °C; divided by 3600 to get kWh.
//      Cp_eff defaults:
//        aqueous             4.18   (water-dominated reactor contents)
//        solid               0.90   (ceramic/metal oxide powder)
//        mixed               2.00   (slurry / powder + liquid binder)
//      Justification: standard engineering handbook values (Perry's §2).
//    E_hold = P_hold × duration_h
//      P_hold = k_type × (T - 25)                    [kW]
//      k_type lookup table (kW·K^-1):
//        muffle_lab_small       0.0012   (5–20 L muffle; datasheets: ~0.9 kW hold @ 800 °C)
//        muffle_lab_large       0.0020   (20–100 L muffle; ~1.5 kW hold @ 800 °C)
//        tube_furnace           0.0015
//        rotary_kiln            0.0060   (industrial; amortized per unit throughput)
//        batch_reactor_jacketed 0.0004   (double-jacketed ~50 L; ~0.4 kW hold @ 50 °C)
//        microwave              0        (use nameplate_kw instead)
//      Calibration sources: Nabertherm / Carbolite lab-furnace datasheets for
//      hold-power vs rated-power; Dunn et al. 2015 (ANL) for industrial NCM
//      calcination SEC ~2-7 kWh/kg; lab-scale expected 5-15 kWh/kg.
//      EIA auxiliary check, not the primary estimator: Ningxia Zhonghua NCM EIA
//      structured result `data/EIA/baseline_UPR/csv/utilities.csv` reports
//      electricity = 133,920,000 kWh/a for 10,000 t/a NCM = 13.392 kWh/kg
//      as a site-wide plant average, not a unit-process calcination value.
//    E_stir = P_stir × duration_h                     (only reactors; default 0.3 kW)
//    For microwave: E_hold = nameplate_kw × duration_h (ignores heat-up).
//    Per-kg basis: divide by product_mass_kg.
//
// 2. Water wash
//    m_wash = m_solid × wash_factor                   [kg]
//    wash_factor defaults (order-of-magnitude, Perry's §18 liquid-solid extraction):
//      coprecipitate          5       (remove mother-liquor salts)
//      post_sinter_rinse      3       (remove residual Li / soluble species)
//      simple_rinse           1.5     (post-drying rinse)
//      aqueous_coating_eia_aux 0.96   (EIA auxiliary fallback only: Ningxia
//                                      Zhonghua 5000 t/a coating line reports
//                                      4800 m3/a coating wastewater = 0.96 kg/kg)
//    Wastewater ≈ m_wash (mass balance, ignoring evaporation).
//
// 3. Oxygen (pure-O2 only)
//    Called only when plan process has `pure_oxygen: true`.
//    V_O2 = flow_Nm3_h × duration_h                   [Nm³]
//    m_O2 = V_O2 × 1.429                              [kg]   (O2 density @ STP)
//    If the patent gives flow_Nm3_h, use it. If it only says pure-O2 atmosphere,
//    default purge is 2 × furnace_volume_m3 per hour for lab/pilot work.
//    For industrial NCM/CAM plans with no disclosed O2 flow, set
//    o2_basis="ncm_cam_eia_auxiliary" to use the EIA auxiliary anchor:
//    Ningxia Zhonghua structured result `data/EIA/baseline_UPR/csv/utilities.csv`
//    reports industrial_O2 = 52,000 t/a for 10,000 t/a NCM = 5.2 kg/kg.
//    Per-kg basis: m_O2 / product_mass_kg.
//
// 4. Waste / emissions (auxiliary EIA factors only)
//    Called when a patent names a waste/emission stream but omits a quantity.
//    These factors are not primary patent data. They are bounded defaults from
//    public EIA/acceptance-monitoring reports for China CAM plants:
//      - Ningxia Zhonghua EIA Table 7-9: PM 1.401 t/a, Ni 0.29864 t/a,
//        Co 0.238434 t/a, Mn 0.270926 t/a at 10,000 t/a NCM.
//      - Bamo acceptance report Table 9.3-1: PM 2.05 t/a and heavy-metal dust
//        1.76 kg/a at 25,000 t/a high-nickel ternary cathode material.
// --------------------------------------------------------------------------

import process from 'node:process';

const HELP = `Usage:
  node patent-to-lifecyclemodel/scripts/estimate-utilities.mjs --mode <electricity|water|oxygen|waste> --params '<json>'

Shared params:
  product_mass_kg        (number, required for electricity/oxygen; water optional)

--mode electricity  params:
  process_type           one of: muffle_lab_small|muffle_lab_large|tube_furnace|
                                 rotary_kiln|batch_reactor_jacketed|microwave
  T_C                    target hold temperature, °C
  duration_h             hold time, hours
  batch_charge_kg        mass heated (reactor contents or furnace charge)
  phase                  aqueous|solid|mixed  (default 'solid' for furnaces, 'aqueous' for reactor)
  stir_kw                optional; defaults 0.3 for batch_reactor_jacketed, 0 else
  nameplate_kw           required for process_type=microwave

--mode water        params:
  solid_mass_kg          kg of solid being washed (per batch)
  wash_regime            coprecipitate|post_sinter_rinse|simple_rinse|
                         aqueous_coating_eia_aux  (default simple_rinse)
  product_mass_kg        kg of final product (for per-kg normalization; defaults to solid_mass_kg)

--mode oxygen       params:
  pure_oxygen            true|false   (must be true, else returns 0 with note)
  duration_h             hours of O2 feeding
  furnace_volume_m3      furnace internal volume (m³)
  flow_Nm3_h             optional explicit flow (Nm³/h); overrides furnace_volume_m3 default
  o2_basis               optional ncm_cam_eia_auxiliary fallback for industrial NCM/CAM
  scale                  optional lab|pilot|industrial, carried into output only
  product_mass_kg        kg of final product

--mode waste       params:
  waste_regime           ncm_calcination_eia_auxiliary
  product_mass_kg        optional kg product; defaults to 1 for per-kg factors
`;

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '-h' || a === '--help') out.help = true;
    else if (a === '--mode') out.mode = argv[++i];
    else if (a === '--params') out.params = argv[++i];
  }
  return out;
}

function die(msg) {
  process.stderr.write(`estimate-utilities: ${msg}\n`);
  process.exit(2);
}

const EIA_AUXILIARY_ANCHORS = {
  ningxia_zhonghua_ncm_2020: {
    source_ref:
      'Ningxia Zhonghua NCM EIA (2020), structured in data/EIA/baseline_UPR; auxiliary only.',
    electricity_kWh_per_kg_site_avg: 13.392,
    industrial_o2_kg_per_kg: 5.2,
    coating_wastewater_kg_per_kg: 0.96,
    fresh_water_makeup_kg_per_kg_site_avg: 25.49,
    collected_or_rework_dust_kg_per_kg_range: [0.00487, 0.00512],
    emitted_pm_kg_per_kg: 0.0001401,
    emitted_ni_kg_per_kg: 0.000029864,
    emitted_co_kg_per_kg: 0.0000238434,
    emitted_mn_kg_per_kg: 0.0000270926,
  },
  bamo_acceptance_2023: {
    source_ref:
      'Guangxi Bamo ternary cathode acceptance monitoring report (2023), Table 9.3-1; auxiliary check only.',
    emitted_pm_kg_per_kg: 0.000082,
    emitted_heavy_metal_dust_kg_per_kg: 0.0000000704,
  },
};

// ---- electricity ---------------------------------------------------------

const K_HOLD = {
  muffle_lab_small: 0.0012,
  muffle_lab_large: 0.0020,
  tube_furnace: 0.0015,
  rotary_kiln: 0.0060,
  batch_reactor_jacketed: 0.0004,
  microwave: 0,
};

const CP_BY_PHASE = { aqueous: 4.18, solid: 0.9, mixed: 2.0 };

export function estimateElectricity(p) {
  const { process_type, T_C, duration_h, batch_charge_kg, product_mass_kg } = p;
  if (!(process_type in K_HOLD)) die(`unknown process_type ${process_type}`);
  if (typeof T_C !== 'number') die('T_C required (°C)');
  if (typeof duration_h !== 'number') die('duration_h required');
  if (typeof batch_charge_kg !== 'number') die('batch_charge_kg required');
  if (typeof product_mass_kg !== 'number' || product_mass_kg <= 0) die('product_mass_kg required (>0)');

  const phase = p.phase ?? (process_type === 'batch_reactor_jacketed' ? 'aqueous' : 'solid');
  const Cp = CP_BY_PHASE[phase];
  if (!Cp) die(`unknown phase ${phase}`);

  const dT = Math.max(T_C - 25, 0);
  const E_heatup = (batch_charge_kg * Cp * dT) / 3600; // kWh

  let E_hold;
  if (process_type === 'microwave') {
    if (typeof p.nameplate_kw !== 'number') die('nameplate_kw required for microwave');
    E_hold = p.nameplate_kw * duration_h;
  } else {
    const k = K_HOLD[process_type];
    const P_hold = k * dT;
    E_hold = P_hold * duration_h;
  }

  const stir_kw = p.stir_kw ?? (process_type === 'batch_reactor_jacketed' ? 0.3 : 0);
  const E_stir = stir_kw * duration_h;

  const E_total_kWh = E_heatup + E_hold + E_stir;
  const kWh_per_kg = E_total_kWh / product_mass_kg;

  return {
    mode: 'electricity',
    kWh_per_kg: round(kWh_per_kg, 4),
    breakdown_kWh_per_batch: {
      heatup: round(E_heatup, 4),
      hold: round(E_hold, 4),
      stirring: round(E_stir, 4),
      total: round(E_total_kWh, 4),
    },
    inputs_used: {
      process_type,
      T_C,
      duration_h,
      batch_charge_kg,
      product_mass_kg,
      phase,
      Cp_kJ_per_kgK: Cp,
      k_hold_kW_per_K: K_HOLD[process_type],
      stir_kw,
      ...(process_type === 'microwave' ? { nameplate_kw: p.nameplate_kw } : {}),
    },
    auxiliary_benchmarks: {
      ncm_cam_site_electricity_kWh_per_kg:
        EIA_AUXILIARY_ANCHORS.ningxia_zhonghua_ncm_2020.electricity_kWh_per_kg_site_avg,
      source_ref: EIA_AUXILIARY_ANCHORS.ningxia_zhonghua_ncm_2020.source_ref,
      note:
        'Use as a reasonableness check only; patent-reported batch conditions and estimator calculation remain primary.',
    },
    formula_ref:
      'E_total = m*Cp*(T-25)/3600 + k_hold*(T-25)*t + P_stir*t; EIA auxiliary check: Ningxia Zhonghua utilities.csv electricity 133,920,000 kWh/a / 10,000 t/a = 13.392 kWh/kg site average.',
  };
}

// ---- water ---------------------------------------------------------------

const WASH_FACTOR = {
  coprecipitate: 5,
  post_sinter_rinse: 3,
  simple_rinse: 1.5,
  aqueous_coating_eia_aux: EIA_AUXILIARY_ANCHORS.ningxia_zhonghua_ncm_2020.coating_wastewater_kg_per_kg,
};

export function estimateWater(p) {
  const { solid_mass_kg } = p;
  if (typeof solid_mass_kg !== 'number' || solid_mass_kg <= 0) die('solid_mass_kg required (>0)');
  const regime = p.wash_regime ?? 'simple_rinse';
  const factor = WASH_FACTOR[regime];
  if (!factor) die(`unknown wash_regime ${regime}`);
  const product_mass_kg = p.product_mass_kg ?? solid_mass_kg;
  if (product_mass_kg <= 0) die('product_mass_kg must be > 0');
  const m_water = solid_mass_kg * factor;
  return {
    mode: 'water',
    kg_water_per_kg: round(m_water / product_mass_kg, 4),
    kg_wastewater_per_kg: round(m_water / product_mass_kg, 4),
    per_batch_kg: { water_in: round(m_water, 3), wastewater_out: round(m_water, 3) },
    inputs_used: { solid_mass_kg, wash_regime: regime, wash_factor: factor, product_mass_kg },
    auxiliary_benchmarks: {
      ncm_cam_aqueous_coating_wastewater_kg_per_kg:
        EIA_AUXILIARY_ANCHORS.ningxia_zhonghua_ncm_2020.coating_wastewater_kg_per_kg,
      ncm_cam_site_fresh_water_makeup_kg_per_kg:
        EIA_AUXILIARY_ANCHORS.ningxia_zhonghua_ncm_2020.fresh_water_makeup_kg_per_kg_site_avg,
      source_ref: EIA_AUXILIARY_ANCHORS.ningxia_zhonghua_ncm_2020.source_ref,
      note:
        'Use coating/wash benchmarks only when the patent gives the relevant step but omits water dose.',
    },
    formula_ref:
      'm_wash = m_solid × wash_factor; wastewater ≈ m_wash. EIA auxiliary coating fallback: Ningxia Zhonghua 5000 t/a line coating wastewater 4800 m3/a / 5000 t/a = 0.96 kg/kg.',
  };
}

// ---- oxygen --------------------------------------------------------------

export function estimateOxygen(p) {
  const pure = p.pure_oxygen === true;
  const product_mass_kg = p.product_mass_kg;
  if (typeof product_mass_kg !== 'number' || product_mass_kg <= 0) die('product_mass_kg required (>0)');
  if (!pure) {
    return {
      mode: 'oxygen',
      kg_O2_per_kg: 0,
      note: 'pure_oxygen is not true → no O2 exchange should be declared',
      inputs_used: { pure_oxygen: pure },
      formula_ref: 'Declare O2 only when source specifies a pure-O2 atmosphere.',
    };
  }
  const { duration_h } = p;
  if (typeof duration_h !== 'number') die('duration_h required');
  if (p.o2_basis === 'ncm_cam_eia_auxiliary') {
    const kgPerKg = EIA_AUXILIARY_ANCHORS.ningxia_zhonghua_ncm_2020.industrial_o2_kg_per_kg;
    return {
      mode: 'oxygen',
      kg_O2_per_kg: kgPerKg,
      per_batch_kg: { o2_in: round(kgPerKg * product_mass_kg, 3) },
      inputs_used: {
        pure_oxygen: true,
        duration_h,
        o2_basis: p.o2_basis,
        scale: p.scale ?? null,
        product_mass_kg,
      },
      auxiliary_benchmarks: {
        ncm_cam_industrial_o2_kg_per_kg: kgPerKg,
        source_ref: EIA_AUXILIARY_ANCHORS.ningxia_zhonghua_ncm_2020.source_ref,
      },
      formula_ref:
        'EIA auxiliary fallback for undisclosed industrial NCM/CAM O2 flow: Ningxia Zhonghua utilities.csv industrial_O2 52,000 t/a / 10,000 t/a NCM = 5.2 kg/kg. Patent remains primary for declaring pure-O2 atmosphere.',
    };
  }
  let flow_Nm3_h = p.flow_Nm3_h;
  if (typeof flow_Nm3_h !== 'number') {
    if (typeof p.furnace_volume_m3 !== 'number') die('flow_Nm3_h or furnace_volume_m3 required');
    flow_Nm3_h = 2 * p.furnace_volume_m3;
  }
  const V = flow_Nm3_h * duration_h;
  const m_O2 = V * 1.429;
  return {
    mode: 'oxygen',
    kg_O2_per_kg: round(m_O2 / product_mass_kg, 4),
    per_batch_kg: { o2_in: round(m_O2, 3) },
    inputs_used: {
      pure_oxygen: true,
      duration_h,
      flow_Nm3_h,
      furnace_volume_m3: p.furnace_volume_m3 ?? null,
      scale: p.scale ?? null,
      product_mass_kg,
    },
    auxiliary_benchmarks: {
      ncm_cam_industrial_o2_kg_per_kg:
        EIA_AUXILIARY_ANCHORS.ningxia_zhonghua_ncm_2020.industrial_o2_kg_per_kg,
      source_ref: EIA_AUXILIARY_ANCHORS.ningxia_zhonghua_ncm_2020.source_ref,
      note:
        'Benchmark only; do not replace a patent-reported O2 flow. Use o2_basis=ncm_cam_eia_auxiliary only when industrial NCM/CAM O2 flow is undisclosed.',
    },
    formula_ref: 'V = flow_Nm3_h × t; m = V × 1.429 kg/Nm³ (O2 @ STP); default flow = 2 furnace-volume turnovers/h when patent gives no flow.',
  };
}

// ---- waste / emissions ---------------------------------------------------

export function estimateWaste(p) {
  const regime = p.waste_regime ?? 'ncm_calcination_eia_auxiliary';
  if (regime !== 'ncm_calcination_eia_auxiliary') die(`unknown waste_regime ${regime}`);
  const product_mass_kg = p.product_mass_kg ?? 1;
  if (typeof product_mass_kg !== 'number' || product_mass_kg <= 0) die('product_mass_kg must be > 0');
  const ningxia = EIA_AUXILIARY_ANCHORS.ningxia_zhonghua_ncm_2020;
  const bamo = EIA_AUXILIARY_ANCHORS.bamo_acceptance_2023;
  const emittedHeavyMetalTotal =
    ningxia.emitted_ni_kg_per_kg + ningxia.emitted_co_kg_per_kg + ningxia.emitted_mn_kg_per_kg;
  return {
    mode: 'waste',
    waste_regime: regime,
    per_kg_product: {
      emitted_PM_to_air_kg: round(ningxia.emitted_pm_kg_per_kg, 10),
      emitted_Ni_to_air_kg: round(ningxia.emitted_ni_kg_per_kg, 10),
      emitted_Co_to_air_kg: round(ningxia.emitted_co_kg_per_kg, 10),
      emitted_Mn_to_air_kg: round(ningxia.emitted_mn_kg_per_kg, 10),
      emitted_heavy_metal_total_to_air_kg: round(emittedHeavyMetalTotal, 10),
      collected_or_rework_dust_kg_range: ningxia.collected_or_rework_dust_kg_per_kg_range,
    },
    per_batch_kg: {
      emitted_PM_to_air: round(ningxia.emitted_pm_kg_per_kg * product_mass_kg, 8),
      emitted_heavy_metal_total_to_air: round(emittedHeavyMetalTotal * product_mass_kg, 8),
    },
    auxiliary_benchmarks: {
      ningxia_zhonghua_design_or_eia_total: {
        source_ref:
          'Ningxia Zhonghua EIA Table 7-9: PM 1.401 t/a, Ni 0.29864 t/a, Co 0.238434 t/a, Mn 0.270926 t/a for 10,000 t/a NCM.',
        emitted_PM_kg_per_kg: ningxia.emitted_pm_kg_per_kg,
        emitted_heavy_metal_total_kg_per_kg: round(emittedHeavyMetalTotal, 10),
      },
      bamo_acceptance_monitoring_check: {
        source_ref: bamo.source_ref,
        emitted_PM_kg_per_kg: bamo.emitted_pm_kg_per_kg,
        emitted_heavy_metal_dust_kg_per_kg: bamo.emitted_heavy_metal_dust_kg_per_kg,
      },
      note:
        'These are auxiliary CAM EIA/acceptance factors. Prefer patent-stated waste amounts, mass balances, or measured emissions when available.',
    },
    formula_ref:
      'EIA auxiliary waste factor: annual pollutant mass / annual product mass; Ningxia PM 1.401 t/a / 10,000 t/a = 0.0001401 kg/kg, Bamo acceptance PM 2.05 t/a / 25,000 t/a = 0.000082 kg/kg.',
  };
}

function round(x, d) {
  const f = 10 ** d;
  return Math.round(x * f) / f;
}

// ---- CLI -----------------------------------------------------------------

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help || !args.mode) {
    process.stdout.write(HELP);
    process.exit(args.help ? 0 : 2);
  }
  let params = {};
  if (args.params) {
    try {
      params = JSON.parse(args.params);
    } catch (e) {
      die(`--params is not valid JSON: ${e.message}`);
    }
  }
  let out;
  switch (args.mode) {
    case 'electricity': out = estimateElectricity(params); break;
    case 'water':       out = estimateWater(params); break;
    case 'oxygen':      out = estimateOxygen(params); break;
    case 'waste':       out = estimateWaste(params); break;
    default: die(`unknown --mode ${args.mode}`);
  }
  process.stdout.write(`${JSON.stringify(out, null, 2)}\n`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
