## How to run
1. Complete the setup in the [repo-root README](../README.md#prerequisites) (Redis, Python, venv, requirements, and `python generator.py` to populate the shared `csv_files/` dataset).
2. `python main.py [none | test_reducer | test_mapper | test_both]` to run the system.
Enabling `test_reducer`/`test_mapper` will crash some reducer/mapper while the system is running. `test_both` will crash both of them.
`none` will crash none of them.

Find the system design in docs/design.md.
Find the deliverables in docs/deliverables.md.
