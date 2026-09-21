"""
Проверяет, что DAG-файлы разбираются планировщиком без ошибок.

Ловит синтаксис, опечатки в импортах, неверные аргументы операторов,
циклы в зависимостях задач.
"""

import sys

from airflow.models import DagBag

EXPECTED_DAGS = {"flight_pipeline"}

dagbag = DagBag(dag_folder="dags", include_examples=False)

if dagbag.import_errors:
    print("DAG не импортируются:\n", file=sys.stderr)
    for path, error in dagbag.import_errors.items():
        print(f"  {path}\n  {error}\n", file=sys.stderr)
    sys.exit(1)

missing = EXPECTED_DAGS - set(dagbag.dags)
if missing:
    print(f"Ожидаемые DAG отсутствуют: {sorted(missing)}", file=sys.stderr)
    sys.exit(1)

for dag_id, dag in sorted(dagbag.dags.items()):
    task_ids = sorted(task.task_id for task in dag.tasks)
    print(f"{dag_id}: задач {len(task_ids)} -> {', '.join(task_ids)}")

print("\nDAG разобраны без ошибок.")
