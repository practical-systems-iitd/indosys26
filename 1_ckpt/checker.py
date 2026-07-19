"""
Checks that the Reducer checkpoints written by the system are consistent
snapshots of the Chandy-Lamport run, cross-checked against what the Mapper
checkpoints say they've actually sent.

Ground truth invariant: the test dataset (csv_files/) contains NUM_FILES csv
files, each made up of rows alternating the words 'a' and 'b' in equal
number, so every file contributes exactly the same count of 'a' and 'b'
(PER_FILE_COUNT each). A mapper only emits a word's count for a file once it
has read that whole file, and each mapper's checkpoint records the id of the
last file (from main.py's global, 1-indexed file counter j) it has read from
its redis stream. Files are assigned to streams round-robin (j % NUM_MAPPERS),
so a mapper's checkpointed id tells us exactly how many files it has fed into
the reducers as of that checkpoint round.

At any consistent global checkpoint, the counts merged across reducers must
satisfy:

  1. count('a') == count('b')                       (rows are symmetric)
  2. count('a') == files_processed_by_mappers * PER_FILE_COUNT
                                        (reducers reflect exactly what the
                                         mappers, at the same round, say they
                                         sent -- no lost/duplicated file)
  3. counts never decrease across increasing checkpoint ids
  4. the final checkpoint (id 0) equals exactly NUM_FILES * PER_FILE_COUNT

Any violation means the checkpointing protocol produced an inconsistent
(non-Chandy-Lamport) snapshot: a lost or double-counted word.
"""
import glob
import json
import re
import sys

from constants import NUM_MAPPERS, NUM_REDUCERS

CKPT_DIR = "checkpoints"
CSV_DIR = "csv_files"
REDUCER_CKPT_RE = re.compile(r"Reducer_(\d+)_(\d+)\.txt$")
MAPPER_CKPT_RE = re.compile(r"Mapper_(\d+)_(\d+)\.txt$")


def dataset_ground_truth() -> tuple[int, int]:
    """Returns (num_files, per_file_count) derived from the actual dataset."""
    files = glob.glob(f"{CSV_DIR}/*.csv")
    if not files:
        raise RuntimeError(f"No csv files found in {CSV_DIR}/")
    with open(files[0]) as f:
        next(f)  # header
        rows = sum(1 for _ in f)
    return len(files), rows * 50


def load_reducer_checkpoints() -> dict[int, dict[int, dict[str, int]]]:
    """checkpoint_id -> {reducer_idx: word_counts}"""
    by_ckpt: dict[int, dict[int, dict[str, int]]] = {}
    for path in glob.glob(f"{CKPT_DIR}/Reducer_*_*.txt"):
        m = REDUCER_CKPT_RE.search(path)
        if not m:
            continue
        r_idx, ckpt_id = int(m.group(1)), int(m.group(2))
        with open(path) as f:
            wc = json.load(f)
        by_ckpt.setdefault(ckpt_id, {})[r_idx] = wc
    return by_ckpt


def load_mapper_checkpoints() -> dict[int, dict[int, int]]:
    """checkpoint_id -> {mapper_idx: last file counter j it has read}"""
    by_ckpt: dict[int, dict[int, int]] = {}
    for path in glob.glob(f"{CKPT_DIR}/Mapper_*_*.txt"):
        m = MAPPER_CKPT_RE.search(path)
        if not m:
            continue
        m_idx, ckpt_id = int(m.group(1)), int(m.group(2))
        with open(path) as f:
            raw = f.read().strip()
        last_j = int(raw.split("-")[0]) if raw else 0
        by_ckpt.setdefault(ckpt_id, {})[m_idx] = last_j
    return by_ckpt


def merge(wc_by_reducer: dict[int, dict[str, int]]) -> dict[str, int]:
    merged: dict[str, int] = {}
    for wc in wc_by_reducer.values():
        for k, v in wc.items():
            merged[k] = merged.get(k, 0) + v
    return merged


def files_processed_by_mapper(last_j: int, mapper_idx: int, num_mappers: int) -> int:
    """Number of files with counter j in [1, last_j] assigned (j % num_mappers)
    to this mapper, i.e. how many files this mapper has read so far."""
    if last_j <= 0:
        return 0
    full_cycles = last_j // num_mappers
    remainder = last_j % num_mappers
    count = full_cycles
    if mapper_idx != 0 and mapper_idx <= remainder:
        count += 1
    return count


def files_processed_at_round(mapper_js: dict[int, int]) -> int:
    return sum(
        files_processed_by_mapper(j, idx, NUM_MAPPERS)
        for idx, j in mapper_js.items()
    )


def check() -> bool:
    num_files, per_file_count = dataset_ground_truth()
    expected_total = num_files * per_file_count
    print(f"Dataset: {num_files} files x {per_file_count} of each word = {expected_total} expected")

    # checkpoint_id -> {reducer_idx: word_counts}
    by_ckpt = load_reducer_checkpoints()
    if not by_ckpt:
        print("FAIL: no reducer checkpoints found")
        return False

    mapper_by_ckpt = load_mapper_checkpoints()

    ok = True
    prev_total = -1
    # Checkpoint ids increase 1, 2, 3, ... during the run; id 0 is the
    # special final checkpoint sent only after every mapper is done.
    ordinary_ids = sorted(k for k in by_ckpt if k != 0)
    ordered_ids = ordinary_ids + ([0] if 0 in by_ckpt else [])

    for ckpt_id in ordered_ids:
        wc_by_reducer = by_ckpt[ckpt_id]

        missing = set(range(NUM_REDUCERS)) - set(wc_by_reducer)
        if missing:
            print(f"[ckpt {ckpt_id}] FAIL: missing checkpoint file(s) for reducer(s) {sorted(missing)}")
            ok = False
            continue

        merged = merge(wc_by_reducer)
        extra_keys = set(merged) - {"a", "b"}
        if extra_keys:
            print(f"[ckpt {ckpt_id}] FAIL: unexpected word(s) {extra_keys} in checkpoint")
            ok = False

        a, b = merged.get("a", 0), merged.get("b", 0)

        if a != b:
            print(f"[ckpt {ckpt_id}] FAIL: count(a)={a} != count(b)={b}, checkpoint is inconsistent")
            ok = False

        mapper_js = mapper_by_ckpt.get(ckpt_id, {})
        missing_mappers = set(range(NUM_MAPPERS)) - set(mapper_js)
        if missing_mappers:
            print(f"[ckpt {ckpt_id}] FAIL: missing checkpoint file(s) for mapper(s) {sorted(missing_mappers)}")
            ok = False
        else:
            files_done = files_processed_at_round(mapper_js)
            expected_a = files_done * per_file_count
            if a != expected_a:
                print(f"[ckpt {ckpt_id}] FAIL: reducers report a={a} ({a // per_file_count} files) but "
                      f"mappers ({mapper_js}) have only sent {files_done} files "
                      f"(expected a={expected_a}) -- reducer/mapper state diverged")
                ok = False

        if a < prev_total:
            print(f"[ckpt {ckpt_id}] FAIL: count(a)={a} decreased from previous checkpoint's {prev_total} "
                  f"-- a word was lost between checkpoints")
            ok = False
        prev_total = max(prev_total, a)

        print(f"[ckpt {ckpt_id}] a={a} b={b} files_accounted={a // per_file_count}/{num_files} "
              f"mapper_js={mapper_js}")

    if 0 in by_ckpt:
        final = merge(by_ckpt[0])
        if final.get("a", 0) != expected_total or final.get("b", 0) != expected_total:
            print(f"FAIL: final checkpoint total a={final.get('a', 0)}, b={final.get('b', 0)}, "
                  f"expected {expected_total} each for all {num_files} files")
            ok = False
    else:
        print("WARNING: no final checkpoint (id 0) found -- run may not have completed")
        ok = False

    return ok


if __name__ == "__main__":
    passed = check()
    print("CONSISTENT" if passed else "INCONSISTENT")
    sys.exit(0 if passed else 1)
