## How to run
1. Complete the setup in the [repo-root README](../README.md#prerequisites) (Redis, Python, venv, requirements, and `python generator.py` to populate the shared `csv_files/` dataset).
2. `python main.py` to run the system.
3. `python checker.py` to verify the produced checkpoints are consistent.


## Coordinator's State Machine

```mermaid
flowchart TD
    A[[CP Phase]] -->|Rx DONE from all mappes| B(LAST_CP Phase)
    A -->|Rx CkptAck from all workers| A
    B -->|Rx last CKPT_ACK from all workers| C(EXIT Phase)
```

## Worker's Internals
![Worker Design](../docs/workers.png "Worker Design")
