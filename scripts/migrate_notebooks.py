"""
migrate_notebooks.py — Update all Spark-ling notebooks for Databricks Connect + S3.

What it does:
1. Replaces SparkSession.builder.master("local[*]") with DatabricksSession (+ local fallback)
2. Replaces spark.read.csv(DATA_RAW / "xxx.csv") with spark.read.parquet (+ CSV fallback)
3. Adds S3_RAW constant and MODE variable
4. Preserves all learning content, outputs, and non-setup cells

Run from project root:
   python scripts/migrate_notebooks.py
"""

import json
import re
import sys
from pathlib import Path

NOTEBOOK_DIR = Path(__file__).parent.parent / "notebooks"
S3_RAW = "s3a://sparkling-data-test/data/raw"

# Map of table names → CSV filenames (used in different notebooks)
TABLE_FILES = {
    "branches": "branches.csv",
    "customers": "customers.csv",
    "accounts": "accounts.csv",
    "transactions": "transactions.csv",
}


def make_spark_setup_cell(app_name: str, extra_configs: dict = None) -> list:
    """Generate a Databricks Connect setup cell with local fallback."""
    lines = [
        "# ── SparkSession: Databricks Connect (remote) / Local fallback ──\n",
        "from pathlib import Path\n",
        "\n",
        "try:\n",
        "    from databricks.connect import DatabricksSession\n",
        f'    spark = DatabricksSession.builder.serverless().getOrCreate()\n',
        "    MODE = 'databricks'\n",
        f'    S3_RAW = "{S3_RAW}"\n',
        f'    print(f"✅ Databricks Connect | Spark {{spark.version}}")\n',
    ]

    lines += [
        "except Exception:\n",
        "    from pyspark.sql import SparkSession\n",
    ]

    # Build local SparkSession with any extra configs
    config_str = ""
    if extra_configs:
        for k, v in extra_configs.items():
            config_str += f'.config("{k}", "{v}")'

    lines += [
        f'    spark = SparkSession.builder.appName("{app_name}").master("local[*]").config("spark.sql.shuffle.partitions", "8"){config_str}.getOrCreate()\n',
        "    MODE = 'local'\n",
        '    S3_RAW = None\n',
        f'    print(f"✅ Local Spark {{spark.version}} | UI: http://localhost:4040")\n',
    ]

    lines += [
        "\n",
        'DATA_RAW = Path("../data/raw")  # local CSV fallback path\n',
        f'print(f"Mode: {{MODE}}")',
    ]
    return lines


def make_read_line(var_name: str, table_name: str, extra_suffix: str = "") -> str:
    """Generate a dual-mode read line for a single table."""
    csv_file = TABLE_FILES.get(table_name, f"{table_name}.csv")
    line = (
        f'{var_name} = spark.read.parquet(f"{{S3_RAW}}/{table_name}") '
        f'if MODE == "databricks" '
        f'else spark.read.csv(str(DATA_RAW / "{csv_file}"), header=True, inferSchema=True)'
    )
    if extra_suffix:
        line += extra_suffix
    return line + "\n"


def find_setup_cell_index(cells):
    """Find the index of the first code cell that sets up Spark."""
    for i, cell in enumerate(cells):
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell["source"])
        if "SparkSession.builder" in src or "get_spark_session" in src:
            return i
    return None


def find_data_load_cell_index(cells, start_after=0):
    """Find the index of the first code cell that loads data via read.csv."""
    for i, cell in enumerate(cells):
        if i <= start_after or cell["cell_type"] != "code":
            continue
        src = "".join(cell["source"])
        if "read.csv" in src and "DATA_RAW" in src:
            return i
    return None


def extract_app_name(cell_source: str) -> str:
    """Extract appName from SparkSession.builder line."""
    m = re.search(r'appName\("([^"]+)"\)', cell_source)
    return m.group(1) if m else "SparklingNotebook"


def extract_extra_configs(cell_source: str) -> dict:
    """Extract extra .config() calls beyond shuffle.partitions."""
    configs = {}
    for m in re.finditer(r'\.config\("([^"]+)",\s*"([^"]+)"\)', cell_source):
        key, val = m.group(1), m.group(2)
        if key != "spark.sql.shuffle.partitions":
            configs[key] = val
    return configs


def extract_data_reads(cell_source: str) -> list:
    """Extract (var_name, table_name, suffix) tuples from read.csv lines."""
    reads = []
    for m in re.finditer(
        r'(\w+)\s*=\s*spark\.read\.csv\(str\(DATA_RAW\s*/\s*"(\w+)\.csv"\),\s*header=True,\s*inferSchema=True\)(.*)',
        cell_source,
    ):
        var_name = m.group(1)
        table_name = m.group(2)
        suffix = m.group(3).strip()
        # Strip trailing quote/comma artifacts
        if suffix and suffix not in (".cache()",):
            suffix = ""
        reads.append((var_name, table_name, suffix))
    return reads


def migrate_notebook(nb_path: Path, dry_run: bool = False) -> bool:
    """Migrate a single notebook. Returns True if modified."""
    with open(nb_path) as f:
        nb = json.load(f)

    cells = nb["cells"]
    src_all = "".join("".join(c.get("source", [])) for c in cells)

    # Skip if already migrated
    if "DatabricksSession" in src_all:
        print(f"  ⏭️  Already migrated, skipping")
        return False

    # Skip if no SparkSession setup found
    setup_idx = find_setup_cell_index(cells)
    if setup_idx is None:
        print(f"  ⚠️  No SparkSession setup found, skipping")
        return False

    setup_src = "".join(cells[setup_idx]["source"])
    app_name = extract_app_name(setup_src)
    extra_configs = extract_extra_configs(setup_src)

    # Replace setup cell
    new_setup_source = make_spark_setup_cell(app_name, extra_configs)
    cells[setup_idx]["source"] = new_setup_source
    cells[setup_idx]["outputs"] = []
    cells[setup_idx]["execution_count"] = None

    # Find and replace data loading cell(s)
    modified_data_cells = 0
    for i, cell in enumerate(cells):
        if i <= setup_idx or cell["cell_type"] != "code":
            continue
        src = "".join(cell["source"])
        if "read.csv" not in src or "DATA_RAW" not in src:
            continue

        reads = extract_data_reads(src)
        if not reads:
            continue

        # Build replacement cell lines
        new_lines = ["# ── Load data (S3 Parquet or local CSV) ──\n"]
        for var_name, table_name, suffix in reads:
            new_lines.append(make_read_line(var_name, table_name, suffix))

        # Preserve any non-read lines (prints, schemas, etc.)
        preserved = []
        for line in cell["source"]:
            stripped = line.strip()
            if (
                stripped
                and "read.csv" not in stripped
                and "DATA_RAW" not in stripped
                and "Path(" not in stripped
                and not stripped.startswith("#")
                and not stripped.startswith("from pathlib")
                and not stripped.startswith("from pyspark.sql import")
                and stripped not in ("", "\\n")
            ):
                preserved.append(line)

        if preserved:
            new_lines.append("\n")
            new_lines.extend(preserved)

        cell["source"] = new_lines
        cell["outputs"] = []
        cell["execution_count"] = None
        modified_data_cells += 1

    print(f"  ✅ Setup cell updated (app={app_name}), {modified_data_cells} data cell(s) updated")

    if not dry_run:
        with open(nb_path, "w") as f:
            json.dump(nb, f, indent=1, ensure_ascii=False)
            f.write("\n")

    return True


def main():
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        print("🔍 DRY RUN — no files will be modified\n")
    else:
        print("🔄 Migrating notebooks for Databricks Connect + S3...\n")

    notebooks = sorted(NOTEBOOK_DIR.glob("*.ipynb"))
    modified = 0

    for nb_path in notebooks:
        print(f"📓 {nb_path.name}")
        try:
            if migrate_notebook(nb_path, dry_run):
                modified += 1
        except Exception as e:
            print(f"  ❌ Error: {e}")

    print(f"\n{'Would modify' if dry_run else 'Modified'}: {modified}/{len(notebooks)} notebooks")
    if not dry_run and modified:
        print("✅ Done! Open the notebooks in VS Code to verify.")


if __name__ == "__main__":
    main()
