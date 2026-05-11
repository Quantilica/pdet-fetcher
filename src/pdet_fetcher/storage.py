from pathlib import Path
from quantilica_core.storage import BaseDataRepository, stamp_filename

class DataRepository(BaseDataRepository):
    """Manages local storage for PDET files using BaseDataRepository."""

    def __init__(self, root: Path | str):
        super().__init__(root)

    def raw_path(self, dataset_id: str, *subkeys: str) -> Path:
        key = "/".join([dataset_id, *subkeys])
        return self.storage.path_for(key)

    def get_docs_filepath(self, file_metadata: dict) -> Path:
        dataset = file_metadata["dataset"]
        filename = get_docs_filename(file_metadata)
        return self.docs_path(dataset, filename)

    def get_caged_filepath(self, file_metadata: dict) -> Path:
        dataset = file_metadata["dataset"]
        year = str(file_metadata["year"])
        filename = get_caged_filename(file_metadata)
        return self.raw_path(dataset, year, filename)

    def get_caged_2020_filepath(self, file_metadata: dict) -> Path:
        dataset = file_metadata["dataset"]
        year = str(file_metadata["year"])
        filename = get_caged_2020_filename(file_metadata)
        return self.raw_path(dataset, year, filename)

    def get_rais_filepath(self, file_metadata: dict) -> Path:
        dataset = file_metadata["dataset"]
        year = str(file_metadata["year"])
        filename = get_rais_filename(file_metadata)
        return self.raw_path(dataset, year, filename)


def get_docs_filename(file_metadata: dict) -> str:
    name, _ = file_metadata["name"].rsplit(".", maxsplit=1)
    modified = file_metadata["datetime"]
    extension = file_metadata["extension"]
    return stamp_filename(name, extension, modified)


def get_docs_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    return DataRepository(dest_dir).get_docs_filepath(file_metadata)


# -----------------------------------------------------------------------------
# ---------------------------------- CAGED ------------------------------------
# -----------------------------------------------------------------------------
def get_caged_filename(file_metadata: dict) -> str:
    dataset = file_metadata["dataset"]
    year = file_metadata["year"]
    partition = f"{year:04}"
    if month := file_metadata.get("month"):
        partition = partition + f"{month:02}"
    modified = file_metadata["datetime"]
    extension = file_metadata["extension"]
    return stamp_filename(f"{dataset}_{partition}", extension, modified)


def get_caged_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    return DataRepository(dest_dir).get_caged_filepath(file_metadata)


def get_caged_docs_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    return get_docs_filepath(file_metadata, dest_dir)


def get_caged_2020_filename(file_metadata: dict) -> str:
    dataset = file_metadata["dataset"]
    year = file_metadata["year"]
    month = file_metadata["month"]
    partition = f"{year:04}{month:02}"
    modified = file_metadata["datetime"]
    extension = file_metadata["extension"]
    return stamp_filename(f"{dataset}_{partition}", extension, modified)


def get_caged_2020_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    return DataRepository(dest_dir).get_caged_2020_filepath(file_metadata)


def get_caged_2020_docs_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    return get_docs_filepath(file_metadata, dest_dir)


# -----------------------------------------------------------------------------
# ----------------------------------- RAIS ------------------------------------
# -----------------------------------------------------------------------------
def get_rais_filename(file_metadata: dict) -> str:
    dataset = file_metadata["dataset"]
    year = file_metadata["year"]
    partition = f"{year}"
    if region := file_metadata.get("uf", file_metadata.get("region")):
        partition = partition + f"-{region}"
    modified = file_metadata["datetime"]
    extension = file_metadata["extension"]
    return stamp_filename(f"{dataset}_{partition}", extension, modified)


def get_rais_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    return DataRepository(dest_dir).get_rais_filepath(file_metadata)


def get_rais_docs_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    return get_docs_filepath(file_metadata, dest_dir)
