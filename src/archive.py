"""archive.py — METIS archive integration module

Provides MetisWISE installation helpers, Podman container management for a
local PostgreSQL + dataserver archive, archive data server client operations,
and master-calibration auto-download logic.  Archive operations call the
MetisWISE Python API directly (the package must be pip-installed into the
project venv).

A local archive pod (managed by Podman) provides both a PostgreSQL database
and a commonwise dataserver.  ``~/.awe/Environment.cfg`` is pointed at
``localhost`` to connect.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Callable

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Section A — MetisWISE availability & installation
# ---------------------------------------------------------------------------


def metiswise_available() -> bool:
    """Return ``True`` if the ``metiswise`` package is importable."""
    try:
        import metiswise  # noqa: F401
        return True
    except ImportError:
        return False


def install_metiswise_command(credentials: str) -> list[str]:
    """Return the ``uv pip install`` command to install MetisWISE.

    Installs into the project ``.venv`` (the same virtual environment
    created by the Install tab via ``uv sync``).  *credentials* should be
    ``"username:password"`` for the OmegaCEN pip channel.
    """
    return [
        "uv", "pip", "install",
        "--python", str(REPO_ROOT / ".venv" / "bin" / "python"),
        "--index-strategy", "unsafe-best-match",
        "--extra-index-url", "https://ftp.eso.org/pub/dfs/pipelines/libraries",
        "--extra-index-url", "https://ivh.github.io/pycpl/simple/",
        "--extra-index-url",
        f"https://{credentials}@pip.entropynaut.com/packages/",
        "metiswise",
    ]


def _ensure_awetarget() -> None:
    """Ensure ``AWETARGET=metiswise`` is set in the environment.

    MetisWISE / commonwise use this variable to locate the correct
    ``Environment.cfg`` shipped with the package.
    """
    os.environ.setdefault("AWETARGET", "metiswise")


DRLD_DIR = REPO_ROOT / "METIS_DRLD"
DRLD_REPO_URL = "https://github.com/AstarVienna/METIS_DRLD.git"

_metiswise_imports_done = False


def _ensure_metiswise_imports() -> None:
    """Ensure the ``codes`` package (METIS_DRLD) is importable, then
    import the full MetisWISE class hierarchy.

    MetisWISE needs ``codes.drld_parser`` from the METIS_DRLD repo to
    enumerate data products and populate ``Raw.class_from_dpr`` (needed
    for uploads).  The DRLD parses ``.tex`` files at runtime, so it must
    be a local clone — ``pip install`` doesn't include them.

    On first call this function clones METIS_DRLD into the project
    directory if it isn't already present, adds it to ``sys.path``, and
    imports ``metiswise.main.aweimports``.
    """
    global _metiswise_imports_done
    if _metiswise_imports_done:
        return
    _metiswise_imports_done = True

    import sys

    # Make the 'codes' package importable.
    drld_path = str(DRLD_DIR)
    if drld_path not in sys.path:
        sys.path.insert(0, drld_path)

    try:
        from codes.drld_parser.data_reduction_library_design import (
            DataReductionLibraryDesign,  # noqa: F401
        )
    except ImportError:
        # METIS_DRLD not cloned yet — fetch it.
        subprocess.run(
            ["git", "clone", "--depth", "1", DRLD_REPO_URL, str(DRLD_DIR)],
            check=True,
        )

    import metiswise.main.aweimports  # noqa: F401


_thread_local = threading.local()


def _ensure_db_connection() -> None:
    """Create a MetisWISE database profile and connection for this thread.

    MetisWISE uses thread-local storage; each new thread must create its
    own profile and database connection.  Repeated calls on the same
    thread are no-ops.
    """
    if getattr(_thread_local, "db_ready", False):
        return
    _ensure_awetarget()
    try:
        from common.config.Profile import profiles
        profiles.create_profile()
        from common.database.Database import database
        database.connect()
    except ImportError:
        pass
    _thread_local.db_ready = True


def reset_db_connection() -> None:
    """Force the next archive operation to re-establish the DB connection.

    Call this after writing new credentials to ``~/.awe/Environment.cfg``
    so that the next ``query_archive`` / ``upload_files`` /
    ``download_file`` call picks up the updated settings.
    """
    _thread_local.db_ready = False


def write_db_credentials(
    username: str,
    password: str,
    *,
    host: str = "",
    data_server: str = "",
    data_port: int = 0,
    data_protocol: str = "https",
) -> Path:
    """Write database credentials to ``~/.awe/Environment.cfg``.

    When *host* is empty the file contains only ``database_user`` and
    ``database_password``, letting all other settings fall through to the
    production config shipped with MetisWISE.

    When *host* is given (e.g. ``"localhost"`` for a local Podman pod) a
    full ``Environment.cfg`` is written that also sets ``database_name``,
    ``database_engine``, ``data_server``, ``data_port``, and ``project``.
    """
    awe_dir = Path.home() / ".awe"
    awe_dir.mkdir(exist_ok=True)
    cfg = awe_dir / "Environment.cfg"

    if host:
        cfg.write_text(
            f"[global]\n"
            f"database_name : {host}/wise\n"
            f"database_engine : postgresql\n"
            f"database_user : {username}\n"
            f"database_password : {password}\n"
            f"data_server : {data_server or host}\n"
            f"data_port : {data_port or 8013}\n"
            f"data_protocol : {data_protocol}\n"
            f"project : SIM\n"
            f"use_n_chars_md5 : 8\n"
            f"mockcommon :\n"
            f"use_find_existing :\n"
            f"use_python_logging : 1\n"
            f"python_logging_level : INFO\n"
        )
    else:
        cfg.write_text(
            f"[global]\n"
            f"database_user : {username}\n"
            f"database_password : {password}\n"
        )
    return cfg


def check_stale_environment_cfg() -> str | None:
    """Return a warning message if ``~/.awe/Environment.cfg`` overrides the
    production config with stale container-era settings, else ``None``.

    ``data_server : localhost`` is *not* considered stale — it is the
    expected value when using the local Podman archive pod.
    """
    cfg = Path.home() / ".awe" / "Environment.cfg"
    if not cfg.exists():
        return None
    try:
        text = cfg.read_text()
        # "dataserver" (the old compose service hostname) is stale;
        # "localhost" is valid (local pod).
        if re.search(r"data_server\s*[:=]\s*dataserver\b", text):
            return (
                f"Warning: {cfg} still points data_server to 'dataserver' "
                f"(the old container hostname).  Consider removing or "
                f"updating {cfg}."
            )
    except OSError:
        pass
    return None


# ---------------------------------------------------------------------------
# Section A2 — Container management (Podman)
# ---------------------------------------------------------------------------

POD_NAME = "metis-archive"
DB_CONTAINER = "metis-archive-db"
DS_CONTAINER = "metis-archive-ds"
ARCHIVE_IMAGE = "metis-archive"
DB_VOLUME = "metis-archive-data"
DS_VOLUME = "metis-archive-space"
CONTAINERFILE = REPO_ROOT / "container" / "archive" / "Containerfile"
CONTAINER_CONTEXT = REPO_ROOT / "container" / "archive"


def podman_available() -> bool:
    """Return ``True`` if ``podman`` is on the PATH."""
    return shutil.which("podman") is not None


def detect_podman_install_cmd() -> list[str]:
    """Return a command to install Podman for the current Linux distro.

    Reads ``/etc/os-release`` to detect the distro family and returns a
    privilege-escalated install command using ``pkexec``.  Raises
    ``RuntimeError`` if the distro cannot be detected.
    """
    os_release = Path("/etc/os-release")
    id_like = ""
    distro_id = ""
    if os_release.exists():
        for line in os_release.read_text().splitlines():
            if line.startswith("ID="):
                distro_id = line.split("=", 1)[1].strip().strip('"')
            elif line.startswith("ID_LIKE="):
                id_like = line.split("=", 1)[1].strip().strip('"')

    ids = {distro_id} | set(id_like.split())

    if ids & {"debian", "ubuntu", "kali"}:
        return ["pkexec", "apt-get", "install", "-y", "podman"]
    if ids & {"fedora", "rhel", "centos"}:
        return ["pkexec", "dnf", "install", "-y", "podman"]
    if ids & {"arch", "manjaro"}:
        return ["pkexec", "pacman", "-S", "--noconfirm", "podman"]

    raise RuntimeError(
        f"Cannot determine Podman install command for distro '{distro_id}' "
        f"(ID_LIKE='{id_like}'). Install Podman manually."
    )


def archive_image_exists() -> bool:
    """Return ``True`` if the ``metis-archive`` container image is built."""
    if not podman_available():
        return False
    result = subprocess.run(
        ["podman", "image", "exists", ARCHIVE_IMAGE],
        capture_output=True,
    )
    return result.returncode == 0


def archive_pod_running() -> bool:
    """Return ``True`` if the ``metis-archive`` pod is running."""
    if not podman_available():
        return False
    result = subprocess.run(
        ["podman", "pod", "inspect", POD_NAME, "--format", "{{.State}}"],
        capture_output=True, text=True,
    )
    return result.returncode == 0 and result.stdout.strip() == "Running"


def db_initialized() -> bool:
    """Return ``True`` if the local archive database has been initialised.

    Checks for the ``aweprojects`` table created by ``dbtestsetup``.
    """
    if not podman_available():
        return False
    result = subprocess.run(
        ["podman", "exec", DB_CONTAINER,
         "psql", "-U", "system", "-d", "wise",
         "-c", "SELECT 1 FROM aweprojects LIMIT 1"],
        capture_output=True,
    )
    return result.returncode == 0


def _stream_process(
    cmd: list[str],
    on_log: Callable[[str], None] | None,
    **kwargs: object,
) -> None:
    """Run *cmd* and stream stdout/stderr lines to *on_log*.

    Raises ``RuntimeError`` on non-zero exit.
    """
    if on_log:
        on_log(f"$ {' '.join(str(c) for c in cmd)}")
    proc = subprocess.Popen(
        [str(c) for c in cmd],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        **kwargs,
    )
    for line in proc.stdout:
        if on_log:
            on_log(line.rstrip("\n"))
    proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(
            f"Command exited {proc.returncode}: {' '.join(str(c) for c in cmd)}"
        )


def build_archive_image(
    credentials: str,
    on_log: Callable[[str], None] | None = None,
) -> None:
    """Build the ``metis-archive`` container image from the Containerfile.

    *credentials* is ``"user:pass"`` for the OmegaCEN package channels.
    The credentials are passed as a Podman build secret and never baked
    into the image itself.
    """
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False
    ) as tmp:
        tmp.write(credentials)
        tmp_path = tmp.name

    try:
        _stream_process(
            [
                "podman", "build",
                "--secret", f"id=OMEGACEN_CREDENTIALS,src={tmp_path}",
                "-t", ARCHIVE_IMAGE,
                "-f", str(CONTAINERFILE),
                str(CONTAINER_CONTEXT),
            ],
            on_log,
        )
    finally:
        os.unlink(tmp_path)


def start_archive_pod(
    on_log: Callable[[str], None] | None = None,
) -> None:
    """Create and start the local archive pod (postgres + dataserver).

    If the pod already exists but is stopped the existing containers are
    started.  On first run the database schema is initialised automatically.
    """
    # If the pod already exists, try to just start it.
    probe = subprocess.run(
        ["podman", "pod", "exists", POD_NAME], capture_output=True,
    )
    if probe.returncode == 0:
        if on_log:
            on_log("Pod already exists — starting…")
        _stream_process(["podman", "pod", "start", POD_NAME], on_log)
        _wait_for_postgres(on_log)
        return

    # Create a new pod with port mappings.
    if on_log:
        on_log("Creating pod…")
    _stream_process(
        [
            "podman", "pod", "create",
            "--name", POD_NAME,
            "--hostname", "localhost",
            "-p", "127.0.0.1:5432:5432",
            "-p", "127.0.0.1:8013:8013",
        ],
        on_log,
    )

    # Start PostgreSQL.
    if on_log:
        on_log("Starting PostgreSQL…")
    _stream_process(
        [
            "podman", "run", "-d",
            "--pod", POD_NAME,
            "--name", DB_CONTAINER,
            "-e", "POSTGRES_DB=wise",
            "-e", "POSTGRES_USER=system",
            "-e", "POSTGRES_PASSWORD=klmn",
            "-v", f"{DB_VOLUME}:/var/lib/postgresql/data",
            "docker.io/library/postgres:17",
        ],
        on_log,
    )

    _wait_for_postgres(on_log)

    # Start the dataserver.
    if on_log:
        on_log("Starting dataserver…")
    _stream_process(
        [
            "podman", "run", "-d",
            "--pod", POD_NAME,
            "--name", DS_CONTAINER,
            "-v", f"{DS_VOLUME}:/root/space",
            f"{ARCHIVE_IMAGE}:latest",
            "/root/scripts/entrypoint_dataserver.sh",
        ],
        on_log,
    )

    # Allow the dataserver a moment to generate its TLS cert and bind.
    time.sleep(2)

    # Initialise the database schema (idempotent — the script checks first).
    # DB_PASSWORD is passed at runtime so it is not baked into the image.
    if on_log:
        on_log("Running database setup…")
    _stream_process(
        ["podman", "exec",
         "-e", "DB_PASSWORD=klmn",
         DS_CONTAINER, "/root/scripts/dbsetup.sh"],
        on_log,
    )


def _wait_for_postgres(
    on_log: Callable[[str], None] | None = None,
    timeout: int = 60,
) -> None:
    """Block until PostgreSQL inside the pod is ready to accept connections."""
    if on_log:
        on_log("Waiting for PostgreSQL to be ready…")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["podman", "exec", DB_CONTAINER,
             "pg_isready", "-U", "system", "-d", "wise"],
            capture_output=True,
        )
        if result.returncode == 0:
            if on_log:
                on_log("PostgreSQL is ready.")
            return
        time.sleep(1)
    raise RuntimeError("PostgreSQL did not become ready within timeout.")


def stop_archive_pod(
    on_log: Callable[[str], None] | None = None,
) -> None:
    """Stop and remove the local archive pod.

    Named volumes (database data, dataserver space) are preserved so that
    the next ``start_archive_pod`` call resumes where it left off.
    """
    probe = subprocess.run(
        ["podman", "pod", "exists", POD_NAME], capture_output=True,
    )
    if probe.returncode != 0:
        if on_log:
            on_log("Pod does not exist — nothing to stop.")
        return

    if on_log:
        on_log("Stopping pod…")
    _stream_process(["podman", "pod", "stop", POD_NAME], on_log)
    if on_log:
        on_log("Removing pod (volumes are preserved)…")
    _stream_process(["podman", "pod", "rm", POD_NAME], on_log)


# ---------------------------------------------------------------------------
# Section B — Archive client (direct MetisWISE API calls)
# ---------------------------------------------------------------------------


def upload_files(
    filepaths: list[Path],
    on_log: Callable[[str], None] | None = None,
) -> list[str]:
    """Upload FITS files to the archive.

    For each file, reads the FITS header to determine whether it is a raw
    or processed data item, then stores and commits it via MetisWISE.

    Returns the list of successfully ingested filenames.
    """
    _ensure_db_connection()
    _ensure_metiswise_imports()

    try:
        from astropy.io import fits
        from metiswise.main.raw import Raw
        from metiswise.main.pro import Pro
    except ImportError as exc:
        raise RuntimeError(
            "MetisWISE is not installed.  Use the Archive tab to install it."
        ) from exc

    ingested: list[str] = []
    total = len(filepaths)

    for i, fp in enumerate(filepaths, 1):
        if on_log:
            on_log(f"[{i}/{total}] Ingesting {fp.name}…")

        try:
            hdus = fits.open(str(fp))
            header = hdus[0].header

            if "ESO DPR CATG" in header:
                di = Raw(str(fp))
            elif "ESO PRO CATG" in header:
                di = Pro(str(fp))
            else:
                if on_log:
                    on_log(
                        f"[{i}/{total}] Skipping {fp.name}: "
                        f"no DPR.CATG or PRO.CATG header"
                    )
                continue

            di.store()
            di.commit()
            ingested.append(fp.name)
            if on_log:
                on_log(f"[{i}/{total}] {fp.name} ingested successfully")

        except Exception as exc:
            if on_log:
                on_log(f"[{i}/{total}] Failed to ingest {fp.name}: {exc}")

    return ingested


def _resolve_dataitem_class(name: str, dataitem_cls: type) -> type | None:
    """Walk DataItem subclasses recursively to find one matching *name*."""
    for sub in dataitem_cls.__subclasses__():
        if sub.__name__ == name:
            return sub
        found = _resolve_dataitem_class(name, sub)
        if found is not None:
            return found
    return None


def query_archive(
    category: str | None = None,
    on_log: Callable[[str], None] | None = None,
) -> list[dict]:
    """Query the archive database for available files.

    If *category* is given it is resolved as a ``DataItem`` subclass name
    (e.g. ``"LINEARITY_2RG"``, ``"IFU_SCI_RAW"``) and ``.select_all()``
    is called on that class.  This works for raw, static-calibration, and
    processed products alike.  Returns a list of dicts with ``filename``,
    ``pro_catg``, and ``class_name`` keys.
    """
    _ensure_db_connection()
    _ensure_metiswise_imports()

    try:
        from metiswise.main.dataitem import DataItem
    except ImportError as exc:
        raise RuntimeError(
            "MetisWISE is not installed.  Use the Archive tab to install it."
        ) from exc

    if on_log:
        on_log("Querying archive…")

    try:
        if category:
            cls = _resolve_dataitem_class(category, DataItem)
            if cls is None:
                if on_log:
                    on_log(f"Unknown category: {category}")
                return []
            results = cls.select_all()
        else:
            results = DataItem.select_all()

        items = []
        for r in results:
            items.append({
                "filename": r.filename,
                "pro_catg": getattr(r, "pro_catg", ""),
                "class_name": type(r).__name__,
            })
        return items

    except Exception as exc:
        if on_log:
            on_log(f"Query failed: {exc}")
        return []


def download_file(
    filename: str,
    dest_dir: Path,
    on_log: Callable[[str], None] | None = None,
) -> Path | None:
    """Download a file from the archive to *dest_dir*.

    Returns the path to the downloaded file, or ``None`` on failure.
    """
    _ensure_db_connection()

    try:
        from metiswise.main.dataitem import DataItem
    except ImportError as exc:
        raise RuntimeError(
            "MetisWISE is not installed.  Use the Archive tab to install it."
        ) from exc

    if on_log:
        on_log(f"Retrieving {filename} from archive…")

    try:
        results = (DataItem.filename == filename)
        if len(results) == 0:
            if on_log:
                on_log(f"File not found in archive: {filename}")
            return None

        di = results[0]
        di.retrieve()

        # Locate the retrieved file.
        src = Path(di.pathname) / di.filename
        if not src.exists():
            src = Path(di.filename)
        if not src.exists():
            if on_log:
                on_log(f"Retrieved file not found on disk: {filename}")
            return None

        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / filename
        shutil.copy2(str(src), str(dest))
        if on_log:
            on_log(f"Downloaded {filename} → {dest}")
        return dest

    except Exception as exc:
        if on_log:
            on_log(f"Download failed: {exc}")
        return None


def list_available_masters(
    workflow: str,
    on_log: Callable[[str], None] | None = None,
) -> dict[str, list[str]]:
    """Query the archive for master calibration files relevant to *workflow*.

    Returns ``{pro_catg: [filename, …]}`` for each master product type
    available in the archive.
    """
    chain = _get_task_chain(workflow)
    result: dict[str, list[str]] = {}
    for task_name, _tag, _meta in chain:
        pro_catg = TASK_TO_MASTER_PROCATG.get(task_name)
        if not pro_catg:
            continue
        items = query_archive(category=pro_catg, on_log=on_log)
        if items:
            result[pro_catg] = [it["filename"] for it in items]
    return result


# ---------------------------------------------------------------------------
# Section C — Master calibration auto-download
# ---------------------------------------------------------------------------

# Maps pipeline task names to the PRO.CATG of the master product they create.
TASK_TO_MASTER_PROCATG: dict[str, str] = {
    # LM IMG
    "metis_lm_img_lingain":          "LINEARITY_2RG",
    "metis_lm_img_dark":             "MASTER_DARK_2RG",
    "metis_lm_img_flat":             "MASTER_IMG_FLAT_LAMP_LM",
    "metis_lm_img_distortion":       "LM_DISTORTION_TABLE",
    # N IMG
    "metis_n_img_lingain":           "LINEARITY_GEO",
    "metis_n_img_dark":              "MASTER_DARK_GEO",
    "metis_n_img_flat":              "MASTER_IMG_FLAT_LAMP_N",
    "metis_n_img_distortion":        "N_DISTORTION_TABLE",
    # IFU
    "metis_ifu_lingain":             "LINEARITY_IFU",
    "metis_ifu_dark":                "MASTER_DARK_IFU",
    "metis_ifu_distortion":          "IFU_DISTORTION_TABLE",
    "metis_ifu_wavecal":             "IFU_WAVECAL",
    "metis_ifu_rsrf":                "MASTER_IFU_RSRF",
    # LM LSS
    "metis_lm_lss_lingain":          "LINEARITY_2RG",
    "metis_lm_lss_dark":             "MASTER_DARK_2RG",
    "metis_lm_lss_rsrf":             "LM_LSS_MASTER_RSRF",
    "metis_lm_lss_trace":            "LM_LSS_TRACE_TABLE",
    "metis_lm_lss_wave":             "LM_LSS_WAVECAL",
    # N LSS
    "metis_n_lss_lingain":           "LINEARITY_GEO",
    "metis_n_lss_dark":              "MASTER_DARK_GEO",
    "metis_n_lss_rsrf":              "N_LSS_MASTER_RSRF",
    "metis_n_lss_trace":             "N_LSS_TRACE_TABLE",
    "metis_n_lss_wave":              "N_LSS_WAVECAL",
}


def _get_task_chain(workflow: str) -> list[tuple[str, str, str | None]]:
    """Import and return the task chain for *workflow* from ``run_metis``."""
    # Late import to avoid circular dependency with run_metis.py.
    from run_metis import WORKFLOW_TASK_CHAIN
    return WORKFLOW_TASK_CHAIN.get(workflow, [])


def _task_covered(task_name: str, raw_tag: str, data_tags: set[str]) -> bool:
    """Return True if *task_name* is satisfied by files the user already has.

    A task is covered when either its raw-input classification tag or its
    master ``PRO.CATG`` appears in *data_tags*.  This lets pre-computed
    master files on disk short-circuit an archive download.
    """
    if raw_tag in data_tags:
        return True
    pro_catg = TASK_TO_MASTER_PROCATG.get(task_name)
    return bool(pro_catg and pro_catg in data_tags)


def identify_missing_calibrations(
    workflow: str,
    data_tags: set[str],
    has_science: bool,
) -> list[tuple[str, str]]:
    """Identify master calibrations needed but not available locally.

    Walks the ``WORKFLOW_TASK_CHAIN`` for *workflow*.  A task is considered
    covered when either its raw-input classification tag or its master
    ``PRO.CATG`` is present in *data_tags*.  For each non-science task that
    is **not** covered but is upstream of one that **is**, the master product
    category is returned.

    Returns a list of ``(task_name, master_pro_catg)`` pairs.
    """
    chain = _get_task_chain(workflow)
    if not chain:
        return []

    # Find the deepest task that is covered (the target).
    deepest_present_idx = -1
    for idx, (task_name, tag, meta) in enumerate(chain):
        if meta == "science":
            continue
        if _task_covered(task_name, tag, data_tags):
            deepest_present_idx = idx

    if deepest_present_idx < 0:
        return []

    # All tasks upstream of (and including) the deepest covered task
    # that are NOT themselves covered need their master products.
    missing: list[tuple[str, str]] = []
    for idx, (task_name, tag, meta) in enumerate(chain):
        if idx > deepest_present_idx:
            break
        if meta == "science":
            continue
        if not _task_covered(task_name, tag, data_tags):
            pro_catg = TASK_TO_MASTER_PROCATG.get(task_name)
            if pro_catg:
                missing.append((task_name, pro_catg))
    return missing


def fetch_missing_calibrations(
    workflow: str,
    data_tags: set[str],
    has_science: bool,
    dest_dir: Path,
    on_log: Callable[[str], None] | None = None,
) -> list[Path]:
    """Download missing master calibration files from the archive.

    1. Calls :func:`identify_missing_calibrations` to find gaps.
    2. Queries the archive for each missing master ``PRO.CATG``.
    3. Downloads matching files into *dest_dir*.

    Returns a list of downloaded file paths.  Gracefully handles the case
    where no master files are available yet (returns an empty list).
    """
    missing = identify_missing_calibrations(workflow, data_tags, has_science)

    if not missing:
        if on_log:
            on_log("No missing calibrations identified")
        return []

    if on_log:
        on_log(f"Missing calibrations: {', '.join(pc for _, pc in missing)}")

    downloaded: list[Path] = []
    for task_name, pro_catg in missing:
        if on_log:
            on_log(f"Searching archive for {pro_catg}…")
        items = query_archive(category=pro_catg, on_log=on_log)
        if not items:
            if on_log:
                on_log(f"  No {pro_catg} found in archive — skipping")
            continue

        # Take the most recent file (last in list).
        target = items[-1]["filename"]
        path = download_file(target, dest_dir, on_log=on_log)
        if path:
            downloaded.append(path)
    return downloaded
