"""Guards against the old/new design drift this repo suffered from."""
import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", "__pycache__", "tests", ".pytest_cache", "target"}


def files(*suffixes):
    return [p for p in ROOT.rglob("*") if p.is_file() and not (set(p.relative_to(ROOT).parts) & SKIP_DIRS)
            and (not suffixes or p.suffix in suffixes or p.name.endswith(suffixes))]


def test_no_legacy_names():
    legacy = re.compile(r"(?<![A-Z_])TRADE_DB\.|TRADE_PIPELINE_WH|TRADE_SIDE|\bQUANTITY\b|TRADE_JSON_FORMAT")
    bad = [str(p.relative_to(ROOT)) for p in files(".sql", ".py", ".yml", ".md", ".tf", ".example")
           if "REVIEW_NOTES" not in p.name and legacy.search(p.read_text(errors="ignore"))]
    assert bad == []


def test_python_compiles():
    for p in files(".py"):
        ast.parse(p.read_text(), filename=str(p))


def test_yaml_parses():
    yaml = __import__("pytest").importorskip("yaml")
    for p in files(".yml"):
        yaml.safe_load(p.read_text())


def test_sql_dollar_quotes_balanced():
    for p in files(".sql"):
        assert p.read_text().count("$$") % 2 == 0, p.name


def test_loader_columns_exist_in_raw_ddl():
    from scripts.load_to_snowflake import COLUMNS
    ddl = (ROOT / "sql/00_bootstrap.sql").read_text()
    raw = ddl.split("RAW.TRADE_EVENTS (")[1].split(");")[0]
    missing = [c for c in COLUMNS if not re.search(rf"\b{c}\b", raw)]
    assert missing == []


def test_generator_fields_match_loader_fields():
    from scripts.generate_trades import FIELDS as G
    from scripts.load_to_snowflake import FIELDS as L
    assert G == L


def test_dbt_refs_resolve():
    models = {p.stem for p in (ROOT / "dbt_project/models").rglob("*.sql")}
    for p in (ROOT / "dbt_project/models").rglob("*.sql"):
        for ref in re.findall(r"ref\('([^']+)'\)", p.read_text()):
            assert ref in models, (p.name, ref)


def test_single_dbt_source_definition():
    srcs = [p for p in (ROOT / "dbt_project").rglob("*.yml") if re.search(r"^sources:", p.read_text(), re.M)]
    assert len(srcs) == 1


def test_airflow_and_makefile_use_real_cli_flags():
    gen = (ROOT / "scripts/generate_trades.py").read_text()
    text = (ROOT / "Makefile").read_text() + (ROOT / "airflow/dags/trade_pipeline_dag.py").read_text()
    for flag in re.findall(r"generate_trades\.py[^\n]*", text):
        for f in re.findall(r"--[a-z-]+", flag):
            assert f'"{f}"' in gen, f
