# IndoSys 2026 Tutorial: Fault Tolerance in Distributed Systems

This repository contains the code for the tutorial presented at **IndoSys 2026**.

## Overview

Distributed systems must be made fault-tolerant, but doing so correctly is
hard: it takes real care to ensure the system keeps producing
**consistent and correct** results even when parts of it fail.

This tutorial explores one of the fundamental techniques for achieving
resiliency: **checkpointing**.

A checkpoint captures the state of a system at a given point in time,
representing all the work completed so far. If the system fails, it can
simply be restarted from the most recent checkpoint instead of starting over
from the beginning.

Through two hands-on assignments, we'll implement checkpointing and use it to
recover a system after a failure.

## Repository Structure

| Directory | Description |
|---|---|
| [`1_ckpt`](./1_ckpt) | Starter code for Assignment 1 — build **consistent** checkpoints. |
| [`1_ckpt_sol`](./1_ckpt_sol) | Reference solution for Assignment 1. |
| [`2_recovery`](./2_recovery) | Starter code for Assignment 2 — use checkpoints to **recover** the system after a failure. |
| [`2_recovery_sol`](./2_recovery_sol) | Reference solution for Assignment 2. |

## Getting Started

### Prerequisites

- **Python** 3.10+
- **Redis** (running locally on the default port `6379`)
- A Python virtual environment with the project dependencies installed:

  ```bash
  python -m venv venv
  source venv/bin/activate
  pip install -r requirements.txt
  ```

- The shared test dataset. Generate it once at the repo root:

  ```bash
  python generator.py
  ```

  This creates `csv_files/` at the root; each assignment directory has a `csv_files` symlink into it, so all four assignments share the same dataset.

### Assignments

1. Start with `1_ckpt` and follow the instructions there to implement
   checkpointing. Compare against `1_ckpt_sol` if you get stuck.
2. Move on to `2_recovery` to implement failure recovery using the
   checkpoints from Assignment 1. Compare against `2_recovery_sol` if
   needed.
