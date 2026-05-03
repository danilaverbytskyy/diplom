import json
from pathlib import Path
from typing import List


RESULTS_DIR = Path('results')
SCENARIOS = ('light', 'heavy')
MODES = ('off', 'local', 'redis', 'multi')


def get_metric_value(metrics: dict, metric_name: str, value_name: str):
    metric = metrics.get(metric_name)
    if not metric:
        return None

    values = metric.get('values')
    if isinstance(values, dict):
        return values.get(value_name)

    return metric.get(value_name)


def round_value(value, digits: int = 2):
    if value is None:
        return '-'

    return round(float(value), digits)


def parse_result(scenario: str, mode: str) -> dict:
    path = RESULTS_DIR / f'results_{scenario}_{mode}.json'

    if not path.exists():
        return {
            'scenario': scenario,
            'mode': mode,
            'avg_ms': '-',
            'p90_ms': '-',
            'p95_ms': '-',
            'p99_ms': '-',
            'rps': '-',
            'failed_rate': '-',
        }

    with path.open('r', encoding='utf-8') as file:
        data = json.load(file)

    metrics = data.get('metrics', {})

    return {
        'scenario': scenario,
        'mode': mode,
        'avg_ms': round_value(get_metric_value(metrics, 'http_req_duration', 'avg')),
        'p90_ms': round_value(get_metric_value(metrics, 'http_req_duration', 'p(90)')),
        'p95_ms': round_value(get_metric_value(metrics, 'http_req_duration', 'p(95)')),
        'p99_ms': round_value(get_metric_value(metrics, 'http_req_duration', 'p(99)')),
        'rps': round_value(get_metric_value(metrics, 'http_reqs', 'rate')),
        'failed_rate': round_value(get_metric_value(metrics, 'http_req_failed', 'rate'), 4),
    }


def print_summary_table(rows: List[dict]) -> None:
    print('| Сценарий | Режим кеша | Avg, мс | p90, мс | p95, мс | p99, мс | RPS | Ошибки |')
    print('|---|---|---:|---:|---:|---:|---:|---:|')

    for row in rows:
        print(
            f'| {row["scenario"]} '
            f'| {row["mode"]} '
            f'| {row["avg_ms"]} '
            f'| {row["p90_ms"]} '
            f'| {row["p95_ms"]} '
            f'| {row["p99_ms"]} '
            f'| {row["rps"]} '
            f'| {row["failed_rate"]} |'
        )


def print_endpoint_metrics() -> None:
    endpoint_metrics = (
        'endpoint_titles_list_duration',
        'endpoint_top_titles_duration',
        'endpoint_search_duration',
        'endpoint_full_title_duration',
        'endpoint_discover_duration',
        'endpoint_top_genres_duration',
    )

    print()
    print('## Метрики по endpoint')
    print()
    print('| Сценарий | Режим | Endpoint metric | Avg, мс | p90, мс | p95, мс | p99, мс |')
    print('|---|---|---|---:|---:|---:|---:|')

    for scenario in SCENARIOS:
        for mode in MODES:
            path = RESULTS_DIR / f'results_{scenario}_{mode}.json'
            if not path.exists():
                continue

            with path.open('r', encoding='utf-8') as file:
                data = json.load(file)

            metrics = data.get('metrics', {})

            for metric_name in endpoint_metrics:
                if metric_name not in metrics:
                    continue

                print(
                    f'| {scenario} '
                    f'| {mode} '
                    f'| {metric_name} '
                    f'| {round_value(get_metric_value(metrics, metric_name, "avg"))} '
                    f'| {round_value(get_metric_value(metrics, metric_name, "p(90)"))} '
                    f'| {round_value(get_metric_value(metrics, metric_name, "p(95)"))} '
                    f'| {round_value(get_metric_value(metrics, metric_name, "p(99)"))} |'
                )


def calculate_improvement(rows: List[dict]) -> None:
    print()
    print('## Улучшение относительно режима off по p95')
    print()
    print('| Сценарий | Режим | p95 off, мс | p95 режима, мс | Улучшение, % |')
    print('|---|---|---:|---:|---:|')

    by_scenario_mode = {
        (row['scenario'], row['mode']): row
        for row in rows
    }

    for scenario in SCENARIOS:
        off_row = by_scenario_mode.get((scenario, 'off'))
        if not off_row or off_row['p95_ms'] == '-':
            continue

        off_p95 = float(off_row['p95_ms'])

        for mode in ('local', 'redis', 'multi'):
            row = by_scenario_mode.get((scenario, mode))
            if not row or row['p95_ms'] == '-':
                continue

            mode_p95 = float(row['p95_ms'])
            improvement = ((off_p95 - mode_p95) / off_p95) * 100 if off_p95 else 0

            print(
                f'| {scenario} '
                f'| {mode} '
                f'| {round(off_p95, 2)} '
                f'| {round(mode_p95, 2)} '
                f'| {round(improvement, 2)} |'
            )


def main() -> None:
    rows = [
        parse_result(scenario, mode)
        for scenario in SCENARIOS
        for mode in MODES
    ]

    print_summary_table(rows)
    print_endpoint_metrics()
    calculate_improvement(rows)


if __name__ == '__main__':
    main()