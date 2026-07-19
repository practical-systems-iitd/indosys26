## How to run
1. Make sure redis server is up and running on port 6379.
2. Make sure you have Python3.10
3. `pip install -r requirements.txt` to install the Python requirements.
4. Run `python generator.py` to generate bunch of csv files in `csv_files` directory.
4. `python main.py [none | test_reducer | test_mapper | test_both]` to run the system.
Enabling `test_reducer`/`test_mapper` will crash some reducer/mapper while the system is running. `test_both` will crash both of them.
`none` will crash none of them.

Find the system design in docs/design.md.
Find the deliverables in docs/deliverables.md.
