#!/usr/bin/env python3
"""Minimal self-check for process chain continuity preflight gate."""

from tiangong_lca_spec.process_from_flow.service import _build_chain_contract, _run_chain_preflight


def _sample_processes():
    return [
        {
            "process_id": "P1",
            "reference_flow_name": "Intermediate A",
            "structure": {"inputs": ["raw x"], "outputs": ["Intermediate A"]},
        },
        {
            "process_id": "P2",
            "reference_flow_name": "Final Product",
            "structure": {"inputs": ["f: Intermediate A"], "outputs": ["Final Product"]},
        },
    ]


def main() -> int:
    processes = _sample_processes()
    contract = _build_chain_contract(processes)

    pass_exchanges = [
        {"process_id": "P1", "exchanges": [{"exchangeName": "Intermediate A", "exchangeDirection": "Output"}]},
        {"process_id": "P2", "exchanges": [{"exchangeName": "F1:   INTERMEDIATE   A", "exchangeDirection": "Input"}]},
    ]
    pass_result = _run_chain_preflight(chain_contract=contract, process_exchanges=pass_exchanges)
    assert pass_result.get("status") == "passed", pass_result

    fail_exchanges = [
        {"process_id": "P1", "exchanges": [{"exchangeName": "Intermediate A", "exchangeDirection": "Output"}]},
        {"process_id": "P2", "exchanges": [{"exchangeName": "Steam", "exchangeDirection": "Input"}]},
    ]
    fail_result = _run_chain_preflight(chain_contract=contract, process_exchanges=fail_exchanges)
    assert fail_result.get("status") == "failed", fail_result
    assert fail_result.get("errors"), fail_result
    assert fail_result["errors"][0].get("code") == "missing_main_input_link", fail_result

    print("selfcheck_chain_preflight: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
