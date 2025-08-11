import pytest
import pandas as pd
import src.extract as mod

def test_extract_csv(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    out_dir  = tmp_path / "tmp"
    data_dir.mkdir()
    out_dir.mkdir()

    monkeypatch.setattr(mod, "DATA_DIR", data_dir)
    monkeypatch.setattr(mod, "OUTPUT_DIR", out_dir)

    csv_file = data_dir / "sample.csv"
    csv_file.write_text("id,value\n1,100\n2,200")

    output_paths = mod.extract_data(["sample.csv"])
    assert len(output_paths) == 1

    df = pd.read_parquet(output_paths[0])
    assert list(df.columns) == ["id", "value"]
    assert df.shape == (2, 2)
    assert df["id"].tolist() == [1, 2]

@pytest.mark.parametrize("bad_input, exc", [
    (None, TypeError),
    (123,  TypeError),
    (["no.csv"], FileNotFoundError),
    ])
def test_extract_errors(tmp_path, monkeypatch, bad_input, exc):
    monkeypatch.setattr(mod, "DATA_DIR", tmp_path)
    monkeypatch.setattr(mod, "OUTPUT_DIR", tmp_path)

    with pytest.raises(exc):
        mod.extract_data(bad_input)