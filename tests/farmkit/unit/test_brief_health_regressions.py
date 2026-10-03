from pathlib import Path

import pytest

from farmkit.brief import lint
from farmkit.health import findings


def _brief(tmp_path, references):
    path = tmp_path / "BRIEF.md"
    path.write_text("## Goal\nTest.\n## Boundaries\nWorkspace only.\n## Read first\n"
                    + references + "\n## Method\nfarmkit tick\n")
    return path


def test_shipped_brief_lints_with_only_required_inputs(tmp_path):
    template = Path(__file__).resolve().parents[3] / "templates" / "BRIEF.md"
    path = tmp_path / "BRIEF.md"
    path.write_text(template.read_text())
    for name in ("steps.toml", "PROTOCOL.md", "NOTES.md", "TRAPS.md"):
        (tmp_path / name).touch()
    assert lint(path) == []


def test_generated_runtime_references_need_not_exist(tmp_path):
    path = _brief(tmp_path, "CHECKPOINT.md\n./CHECKPOINT.md\n.farm_receipt.py\n./.farm_receipt.py\n"
                           "attempts/\nattempts/result.json\n.farm/.\n.farm/tasks/x.json")
    assert lint(path) == []


@pytest.mark.parametrize("reference", ["OPTIONAL.md if it exists", "`OPTIONAL.md` when present",
                                        "OPTIONAL.md (if present)"])
def test_optional_reference_is_not_required(tmp_path, reference):
    assert lint(_brief(tmp_path, reference)) == []


def test_optional_qualifier_does_not_hide_another_required_reference(tmp_path):
    assert lint(_brief(tmp_path, "OPTIONAL.md if it exists; REQUIRED.md")) == [
        "referenced path does not exist: REQUIRED.md"]


def test_directory_punctuation_and_web_links(tmp_path):
    (tmp_path / "inputs").mkdir()
    assert lint(_brief(tmp_path, "inputs/.\nhttps://example.org/missing/file.md")) == []
    assert lint(_brief(tmp_path, "missing/.")) == ["referenced path does not exist: missing/"]


@pytest.mark.parametrize("matches, expected", [(None, "unknown"), (True, "ok"), (False, "fail")])
def test_manifest_comparison_preserves_unknown(matches, expected):
    result = next(f for f in findings({"source_matches": matches}) if f["check"] == "code matches manifest")
    assert result["level"] == expected
    if matches is None:
        assert result["detail"] == "no daemon manifest yet"
