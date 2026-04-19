"""
Unit tests for archive.py.

Covers:
  - MetisWISE availability check
  - Install command generation
  - Stale Environment.cfg detection
  - Podman availability & install command detection
  - Container image / pod management helpers
  - Database credential writing (remote & local modes)
  - Upload / query / download with mocked MetisWISE
  - Missing calibration identification
"""

import shutil
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

import archive

# Mocks for the commonwise database modules imported by _ensure_db_connection().
# Merge into every patch.dict("sys.modules", …) block that exercises upload,
# query, or download functions.
_DB_MOCKS = {
    "common": MagicMock(),
    "common.config": MagicMock(),
    "common.config.Profile": MagicMock(),
    "common.database": MagicMock(),
    "common.database.Database": MagicMock(),
}

# Mocks for the modules imported by _ensure_metiswise_imports().
_IMPORT_MOCKS = {
    "codes": MagicMock(),
    "codes.drld_parser": MagicMock(),
    "codes.drld_parser.data_reduction_library_design": MagicMock(),
    "metiswise.main.aweimports": MagicMock(),
}


# ---------------------------------------------------------------------------
# MetisWISE availability
# ---------------------------------------------------------------------------


class TestMetisWISEAvailable:
    def test_available(self):
        with patch.dict("sys.modules", {"metiswise": MagicMock()}):
            assert archive.metiswise_available() is True

    def test_not_installed(self):
        with patch.dict("sys.modules", {"metiswise": None}):
            assert archive.metiswise_available() is False


# ---------------------------------------------------------------------------
# Install command
# ---------------------------------------------------------------------------


class TestInstallMetisWiseCommand:
    def test_command_structure(self):
        cmd = archive.install_metiswise_command("user:secret")
        assert cmd[0:3] == ["uv", "pip", "install"]
        assert "--python" in cmd
        assert "metiswise" in cmd

    def test_credentials_in_url(self):
        cmd = archive.install_metiswise_command("alice:p4ss")
        urls = [arg for arg in cmd if "entropynaut" in arg]
        assert len(urls) == 1
        assert "alice:p4ss@pip.entropynaut.com" in urls[0]

    def test_extra_index_urls(self):
        cmd = archive.install_metiswise_command("u:p")
        idx_flags = [i for i, a in enumerate(cmd) if a == "--extra-index-url"]
        assert len(idx_flags) == 3
        urls = [cmd[i + 1] for i in idx_flags]
        assert any("ftp.eso.org" in u for u in urls)
        assert any("ivh.github.io" in u for u in urls)
        assert any("entropynaut" in u for u in urls)

    def test_uses_unsafe_best_match(self):
        cmd = archive.install_metiswise_command("u:p")
        assert "--index-strategy" in cmd
        idx = cmd.index("--index-strategy")
        assert cmd[idx + 1] == "unsafe-best-match"

    def test_targets_project_venv(self):
        cmd = archive.install_metiswise_command("u:p")
        python_idx = cmd.index("--python")
        python_path = cmd[python_idx + 1]
        assert ".venv" in python_path
        assert python_path.endswith("python")

    def test_does_not_include_pymetis(self):
        cmd = archive.install_metiswise_command("u:p")
        git_args = [a for a in cmd if a.startswith("git+")]
        assert len(git_args) == 0


# ---------------------------------------------------------------------------
# Stale Environment.cfg detection
# ---------------------------------------------------------------------------


class TestCheckStaleEnvironmentCfg:
    def test_no_file(self, tmp_path):
        with patch("archive.Path.home", return_value=tmp_path):
            assert archive.check_stale_environment_cfg() is None

    def test_production_config_no_warning(self, tmp_path):
        awe = tmp_path / ".awe"
        awe.mkdir()
        (awe / "Environment.cfg").write_text(
            "data_server : metis-ds.hpc.rug.nl\n"
        )
        with patch("archive.Path.home", return_value=tmp_path):
            assert archive.check_stale_environment_cfg() is None

    def test_stale_container_config_warns(self, tmp_path):
        awe = tmp_path / ".awe"
        awe.mkdir()
        (awe / "Environment.cfg").write_text(
            "data_server : dataserver\ndata_port : 8013\n"
        )
        with patch("archive.Path.home", return_value=tmp_path):
            warning = archive.check_stale_environment_cfg()
            assert warning is not None
            assert "dataserver" in warning

    def test_localhost_not_stale(self, tmp_path):
        awe = tmp_path / ".awe"
        awe.mkdir()
        (awe / "Environment.cfg").write_text(
            "data_server : localhost\ndata_port : 8013\n"
        )
        with patch("archive.Path.home", return_value=tmp_path):
            assert archive.check_stale_environment_cfg() is None


# ---------------------------------------------------------------------------
# Database connection setup
# ---------------------------------------------------------------------------


class TestEnsureDbConnection:
    def test_creates_profile_and_connects(self):
        mock_profiles = MagicMock()
        mock_database = MagicMock()

        with patch.dict("sys.modules", {
            "common": MagicMock(),
            "common.config": MagicMock(),
            "common.config.Profile": MagicMock(profiles=mock_profiles),
            "common.database": MagicMock(),
            "common.database.Database": MagicMock(database=mock_database),
        }):
            import importlib
            importlib.reload(archive)

            archive._ensure_db_connection()
            mock_profiles.create_profile.assert_called_once()
            mock_database.connect.assert_called_once()

            importlib.reload(archive)

    def test_idempotent_within_thread(self):
        mock_profiles = MagicMock()
        mock_database = MagicMock()

        with patch.dict("sys.modules", {
            "common": MagicMock(),
            "common.config": MagicMock(),
            "common.config.Profile": MagicMock(profiles=mock_profiles),
            "common.database": MagicMock(),
            "common.database.Database": MagicMock(database=mock_database),
        }):
            import importlib
            importlib.reload(archive)

            archive._ensure_db_connection()
            archive._ensure_db_connection()
            # Only called once despite two invocations
            mock_profiles.create_profile.assert_called_once()
            mock_database.connect.assert_called_once()

            importlib.reload(archive)

    def test_noop_when_commonwise_missing(self):
        # No common.* modules mocked — ImportError path fires
        import importlib
        importlib.reload(archive)

        # Should not raise; just sets db_ready and returns
        archive._ensure_db_connection()
        assert archive._thread_local.db_ready is True

        importlib.reload(archive)


class TestResetDbConnection:
    def test_clears_flag(self):
        archive._thread_local.db_ready = True
        archive.reset_db_connection()
        assert not getattr(archive._thread_local, "db_ready", False)


class TestWriteDbCredentials:
    def test_writes_config(self, tmp_path):
        with patch("archive.Path.home", return_value=tmp_path):
            cfg = archive.write_db_credentials("AWAITTEST", "secret")
        assert cfg.exists()
        text = cfg.read_text()
        assert text.startswith("[global]\n")
        assert "database_user : AWAITTEST" in text
        assert "database_password : secret" in text

    def test_creates_awe_dir(self, tmp_path):
        with patch("archive.Path.home", return_value=tmp_path):
            archive.write_db_credentials("user", "pass")
        assert (tmp_path / ".awe").is_dir()

    def test_remote_mode_minimal(self, tmp_path):
        """Without host=, only username and password are written."""
        with patch("archive.Path.home", return_value=tmp_path):
            cfg = archive.write_db_credentials("USER", "PW")
        text = cfg.read_text()
        assert "database_name" not in text
        assert "data_server" not in text

    def test_local_mode_full_config(self, tmp_path):
        """With host='localhost', a complete Environment.cfg is written."""
        with patch("archive.Path.home", return_value=tmp_path):
            cfg = archive.write_db_credentials(
                "AWTEST", "lmno", host="localhost",
            )
        text = cfg.read_text()
        assert "database_name : localhost/wise" in text
        assert "database_engine : postgresql" in text
        assert "database_user : AWTEST" in text
        assert "database_password : lmno" in text
        assert "data_server : localhost" in text
        assert "data_port : 8013" in text
        assert "data_protocol : https" in text
        assert "project : SIM" in text

    def test_local_mode_custom_data_server(self, tmp_path):
        with patch("archive.Path.home", return_value=tmp_path):
            cfg = archive.write_db_credentials(
                "U", "P", host="myhost",
                data_server="ds.example.com", data_port=9999,
            )
        text = cfg.read_text()
        assert "database_name : myhost/wise" in text
        assert "data_server : ds.example.com" in text
        assert "data_port : 9999" in text


# ---------------------------------------------------------------------------
# Podman availability & install command
# ---------------------------------------------------------------------------


class TestPodmanAvailable:
    def test_available(self):
        with patch("archive.shutil.which", return_value="/usr/bin/podman"):
            assert archive.podman_available() is True

    def test_not_available(self):
        with patch("archive.shutil.which", return_value=None):
            assert archive.podman_available() is False


class TestDetectPodmanInstallCmd:
    def _mock_os_release(self, tmp_path, distro_id, id_like=""):
        content = f'ID={distro_id}\n'
        if id_like:
            content += f'ID_LIKE="{id_like}"\n'
        (tmp_path / "os-release").write_text(content)
        return tmp_path / "os-release"

    def _run_with_os_release(self, tmp_path, content):
        """Write a fake /etc/os-release and run detect_podman_install_cmd."""
        os_release = tmp_path / "os-release"
        os_release.write_text(content)
        with patch("archive.Path", return_value=os_release):
            return archive.detect_podman_install_cmd()

    def test_debian(self, tmp_path):
        cmd = self._run_with_os_release(tmp_path, 'ID=debian\n')
        assert cmd == ["pkexec", "apt-get", "install", "-y", "podman"]

    def test_ubuntu(self, tmp_path):
        cmd = self._run_with_os_release(tmp_path, 'ID=ubuntu\nID_LIKE="debian"\n')
        assert "apt-get" in cmd

    def test_kali(self, tmp_path):
        cmd = self._run_with_os_release(tmp_path, 'ID=kali\nID_LIKE="debian"\n')
        assert "apt-get" in cmd

    def test_fedora(self, tmp_path):
        cmd = self._run_with_os_release(tmp_path, 'ID=fedora\n')
        assert "dnf" in cmd

    def test_arch(self, tmp_path):
        cmd = self._run_with_os_release(tmp_path, 'ID=arch\n')
        assert "pacman" in cmd

    def test_unknown_raises(self, tmp_path):
        os_release = tmp_path / "os-release"
        os_release.write_text('ID=obscure\n')
        with patch("archive.Path", return_value=os_release):
            with pytest.raises(RuntimeError, match="Cannot determine"):
                archive.detect_podman_install_cmd()


# ---------------------------------------------------------------------------
# Container image & pod status helpers
# ---------------------------------------------------------------------------


class TestArchiveImageExists:
    def test_exists(self):
        with patch("archive.podman_available", return_value=True), \
             patch("archive.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            assert archive.archive_image_exists() is True
            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert cmd[:3] == ["podman", "image", "exists"]

    def test_not_exists(self):
        with patch("archive.podman_available", return_value=True), \
             patch("archive.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            assert archive.archive_image_exists() is False

    def test_no_podman(self):
        with patch("archive.podman_available", return_value=False):
            assert archive.archive_image_exists() is False


class TestArchivePodRunning:
    def test_running(self):
        with patch("archive.podman_available", return_value=True), \
             patch("archive.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0, stdout="Running\n",
            )
            assert archive.archive_pod_running() is True

    def test_stopped(self):
        with patch("archive.podman_available", return_value=True), \
             patch("archive.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0, stdout="Exited\n",
            )
            assert archive.archive_pod_running() is False

    def test_no_pod(self):
        with patch("archive.podman_available", return_value=True), \
             patch("archive.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=125)
            assert archive.archive_pod_running() is False

    def test_no_podman(self):
        with patch("archive.podman_available", return_value=False):
            assert archive.archive_pod_running() is False


class TestDbInitialized:
    def test_initialized(self):
        with patch("archive.podman_available", return_value=True), \
             patch("archive.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            assert archive.db_initialized() is True

    def test_not_initialized(self):
        with patch("archive.podman_available", return_value=True), \
             patch("archive.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=2)
            assert archive.db_initialized() is False

    def test_no_podman(self):
        with patch("archive.podman_available", return_value=False):
            assert archive.db_initialized() is False


# ---------------------------------------------------------------------------
# Upload files (mocked MetisWISE)
# ---------------------------------------------------------------------------


class TestUploadFiles:
    def test_upload_raw_file(self, tmp_path):
        fits_file = tmp_path / "raw.fits"
        fits_file.write_bytes(b"dummy")

        mock_fits = MagicMock()
        mock_hdus = MagicMock()
        mock_hdus.__getitem__ = MagicMock(
            return_value=MagicMock(header={"ESO DPR CATG": "FLAT"})
        )
        mock_fits.open.return_value = mock_hdus

        mock_raw_cls = MagicMock()
        mock_raw_inst = MagicMock()
        mock_raw_cls.return_value = mock_raw_inst

        mock_astropy_io = MagicMock(fits=mock_fits)
        mock_mw_main = MagicMock()
        mock_mw_main.raw = MagicMock(Raw=mock_raw_cls)
        mock_mw_main.pro = MagicMock()

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "astropy": MagicMock(),
            "astropy.io": mock_astropy_io,
            "astropy.io.fits": mock_fits,
            "metiswise": MagicMock(),
            "metiswise.main": mock_mw_main,
            "metiswise.main.raw": MagicMock(Raw=mock_raw_cls),
            "metiswise.main.pro": MagicMock(),
        }):
            import importlib
            importlib.reload(archive)

            result = archive.upload_files([fits_file])
            assert result == ["raw.fits"]
            mock_raw_inst.store.assert_called_once()
            mock_raw_inst.commit.assert_called_once()

            importlib.reload(archive)

    def test_upload_skips_unknown_header(self, tmp_path):
        fits_file = tmp_path / "unknown.fits"
        fits_file.write_bytes(b"dummy")

        mock_fits = MagicMock()
        mock_hdus = MagicMock()
        mock_hdus.__getitem__ = MagicMock(
            return_value=MagicMock(header={})
        )
        mock_fits.open.return_value = mock_hdus

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "astropy": MagicMock(),
            "astropy.io": MagicMock(),
            "astropy.io.fits": mock_fits,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.raw": MagicMock(),
            "metiswise.main.pro": MagicMock(),
        }):
            import importlib
            importlib.reload(archive)

            logs = []
            result = archive.upload_files([fits_file], on_log=logs.append)
            assert result == []
            assert any("Skipping" in msg for msg in logs)

            importlib.reload(archive)


# ---------------------------------------------------------------------------
# Query archive (mocked MetisWISE)
# ---------------------------------------------------------------------------


class TestQueryArchive:
    def test_query_all(self):
        mock_item = MagicMock()
        mock_item.filename = "test.fits"
        mock_item.pro_catg = "MASTER_DARK"
        type(mock_item).__name__ = "Pro"

        mock_dataitem = MagicMock()
        mock_dataitem.select_all.return_value = [mock_item]

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=mock_dataitem),
            "metiswise.main.pro": MagicMock(),
        }):
            import importlib
            importlib.reload(archive)

            items = archive.query_archive()
            assert len(items) == 1
            assert items[0]["filename"] == "test.fits"

            importlib.reload(archive)

    def test_query_by_category(self):
        mock_item = MagicMock()
        mock_item.filename = "master.fits"
        mock_item.pro_catg = "MASTER_DARK"

        class _DataItem:
            pass

        class MASTER_DARK(_DataItem):
            @classmethod
            def select_all(cls):
                return [mock_item]

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=_DataItem),
        }):
            import importlib
            importlib.reload(archive)

            items = archive.query_archive(category="MASTER_DARK")
            assert len(items) == 1
            assert items[0]["filename"] == "master.fits"

            importlib.reload(archive)

    def test_query_raw_category(self):
        mock_item = MagicMock()
        mock_item.filename = "raw.fits"
        mock_item.pro_catg = AttributeError  # raw items have no pro_catg
        del mock_item.pro_catg

        class _DataItem:
            pass

        class IFU_SCI_RAW(_DataItem):
            @classmethod
            def select_all(cls):
                return [mock_item]

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=_DataItem),
        }):
            import importlib
            importlib.reload(archive)

            items = archive.query_archive(category="IFU_SCI_RAW")
            assert len(items) == 1
            assert items[0]["filename"] == "raw.fits"
            assert items[0]["pro_catg"] == ""

            importlib.reload(archive)

    def test_query_unknown_category(self):
        class _DataItem:
            pass

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=_DataItem),
        }):
            import importlib
            importlib.reload(archive)

            logs = []
            items = archive.query_archive(
                category="NONEXISTENT", on_log=logs.append,
            )
            assert items == []
            assert any("Unknown category" in msg for msg in logs)

            importlib.reload(archive)

    def test_query_resolves_nested_subclass(self):
        mock_item = MagicMock()
        mock_item.filename = "nested.fits"
        mock_item.pro_catg = "LINEARITY_2RG"

        class _DataItem:
            pass

        class _Intermediate(_DataItem):
            pass

        class LINEARITY_2RG(_Intermediate):
            @classmethod
            def select_all(cls):
                return [mock_item]

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=_DataItem),
        }):
            import importlib
            importlib.reload(archive)

            items = archive.query_archive(category="LINEARITY_2RG")
            assert len(items) == 1
            assert items[0]["filename"] == "nested.fits"

            importlib.reload(archive)


# ---------------------------------------------------------------------------
# Download file (mocked MetisWISE)
# ---------------------------------------------------------------------------


class TestDownloadFile:
    def test_download_success(self, tmp_path):
        # Create a fake source file that MetisWISE would "retrieve"
        src_dir = tmp_path / "retrieve_dir"
        src_dir.mkdir()
        (src_dir / "data.fits").write_bytes(b"fits data")

        mock_item = MagicMock()
        mock_item.filename = "data.fits"
        mock_item.pathname = str(src_dir)
        mock_item.retrieve = MagicMock()

        mock_dataitem = MagicMock()
        mock_dataitem.filename.__eq__ = MagicMock(return_value=[mock_item])

        dest_dir = tmp_path / "downloads"

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=mock_dataitem),
        }):
            import importlib
            importlib.reload(archive)

            result = archive.download_file("data.fits", dest_dir)
            assert result is not None
            assert result == dest_dir / "data.fits"
            assert result.exists()
            mock_item.retrieve.assert_called_once()

            importlib.reload(archive)

    def test_download_not_found(self, tmp_path):
        mock_dataitem = MagicMock()
        mock_dataitem.filename.__eq__ = MagicMock(return_value=[])

        dest_dir = tmp_path / "downloads"

        with patch.dict("sys.modules", {
            **_DB_MOCKS,
            **_IMPORT_MOCKS,
            "metiswise": MagicMock(),
            "metiswise.main": MagicMock(),
            "metiswise.main.dataitem": MagicMock(DataItem=mock_dataitem),
        }):
            import importlib
            importlib.reload(archive)

            result = archive.download_file("missing.fits", dest_dir)
            assert result is None

            importlib.reload(archive)


# ---------------------------------------------------------------------------
# Missing calibration identification
# ---------------------------------------------------------------------------


class TestIdentifyMissingCalibrations:
    """Test the pure-logic calibration gap detection."""

    def test_no_gaps_when_all_present(self):
        # IFU workflow: provide all raw tags
        all_tags = {
            "DETLIN_IFU_RAW", "DARK_IFU_RAW", "IFU_DISTORTION_RAW",
            "IFU_WAVE_RAW", "IFU_RSRF_RAW", "IFU_STD_RAW",
        }
        missing = archive.identify_missing_calibrations(
            "metis.metis_ifu_wkf", all_tags, has_science=False,
        )
        assert missing == []

    def test_detects_upstream_gap(self):
        # Only have the rsrf raw — lingain, dark, distortion, wavecal
        # are upstream and missing.
        missing = archive.identify_missing_calibrations(
            "metis.metis_ifu_wkf",
            data_tags={"IFU_RSRF_RAW"},
            has_science=False,
        )
        task_names = [t for t, _ in missing]
        assert "metis_ifu_lingain" in task_names
        assert "metis_ifu_dark" in task_names
        assert "metis_ifu_distortion" in task_names
        assert "metis_ifu_wavecal" in task_names
        # rsrf itself is present, so it should NOT be listed
        assert "metis_ifu_rsrf" not in task_names

    def test_lm_img_partial(self):
        # Only have flat raw — lingain and dark are missing
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={"LM_FLAT_LAMP_RAW"},
            has_science=False,
        )
        task_names = [t for t, _ in missing]
        assert "metis_lm_img_lingain" in task_names
        assert "metis_lm_img_dark" in task_names
        assert "metis_lm_img_flat" not in task_names

    def test_empty_data_tags(self):
        missing = archive.identify_missing_calibrations(
            "metis.metis_ifu_wkf", set(), has_science=False,
        )
        assert missing == []

    def test_unknown_workflow(self):
        missing = archive.identify_missing_calibrations(
            "metis.nonexistent_wkf", {"FOO"}, has_science=False,
        )
        assert missing == []

    def test_science_tasks_ignored(self):
        # Science tasks should not contribute to the missing list
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={"LM_DISTORTION_RAW", "LM_IMAGE_SCI_RAW"},
            has_science=True,
        )
        task_names = [t for t, _ in missing]
        # lingain, dark, flat are upstream of distortion
        assert "metis_lm_img_lingain" in task_names
        assert "metis_lm_img_dark" in task_names
        assert "metis_lm_img_flat" in task_names
        # Science tasks should not appear
        assert "metis_lm_img_basic_reduce_sci" not in task_names
        assert "metis_lm_img_basic_reduce_std" not in task_names

    def test_master_pro_catg_covers_task(self):
        # User has flat raw + master dark + master linearity: nothing to fetch
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={"LM_FLAT_LAMP_RAW", "MASTER_DARK_2RG", "LINEARITY_2RG"},
            has_science=False,
        )
        assert missing == []

    def test_master_fills_partial_gap(self):
        # Flat raw + master dark, but no linearity master or raw — only
        # lingain should still be listed as missing.
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={"LM_FLAT_LAMP_RAW", "MASTER_DARK_2RG"},
            has_science=False,
        )
        task_names = [t for t, _ in missing]
        assert task_names == ["metis_lm_img_lingain"]
        assert missing == [("metis_lm_img_lingain", "LINEARITY_2RG")]

    def test_only_masters_no_raw(self):
        # All-masters coverage up through flat: flat is the deepest covered
        # task (via its PRO.CATG), and dark + lingain are also covered.
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={
                "MASTER_DARK_2RG",
                "LINEARITY_2RG",
                "MASTER_IMG_FLAT_LAMP_LM",
            },
            has_science=False,
        )
        assert missing == []

    def test_raw_and_master_same_task(self):
        # Providing both the raw and the master for dark must not confuse
        # the gap walk — lingain is still missing.
        missing = archive.identify_missing_calibrations(
            "metis.metis_lm_img_wkf",
            data_tags={"DARK_2RG_RAW", "MASTER_DARK_2RG"},
            has_science=False,
        )
        task_names = [t for t, _ in missing]
        assert task_names == ["metis_lm_img_lingain"]
        assert "metis_lm_img_dark" not in task_names
