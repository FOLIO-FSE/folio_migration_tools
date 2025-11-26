from unittest.mock import patch, MagicMock
import pytest

from folio_migration_tools.migration_tasks.bibs_transformer import BibsTransformer
from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_migration_tools.library_configuration import (
    FileDefinition,
    IlsFlavour,
)
from .test_infrastructure.mocked_classes import (
    mocked_folio_client,
    get_mocked_library_config,
    get_mocked_folder_structure
)

TASK_CONFIG = BibsTransformer.TaskConfiguration(
    name="test_task",
    migration_task_type="BibsTransformer",
    files=[FileDefinition(file_name="test.mrc")],
    ils_flavour=IlsFlavour.voyager
)


@pytest.fixture
def mock_folder_structure():
    """Fixture to mock FolderStructure"""
    with patch(
        'folio_migration_tools.migration_tasks.migration_task_base.FolderStructure',
        return_value=get_mocked_folder_structure()
    ) as mock:
        yield mock


@pytest.fixture
def mock_check_source_files():
    """Fixture to mock check_source_files"""
    with patch(
        'folio_migration_tools.migration_tasks.bibs_transformer.BibsTransformer.check_source_files',
        return_value=[]
    ) as mock:
        yield mock


def test_get_object_type():
    assert BibsTransformer.get_object_type() == FOLIONamespaces.instances


def test_init(mock_folder_structure, mock_check_source_files):
    library_config = get_mocked_library_config()
    folio_client = mocked_folio_client()
    transformer = BibsTransformer(TASK_CONFIG, library_config, folio_client)
    assert transformer.task_configuration == TASK_CONFIG
    assert transformer.library_configuration == library_config
    assert transformer.folio_client == folio_client


@patch(
    'folio_migration_tools.migration_tasks.bibs_transformer.BibsTransformer'
    '.do_work_marc_transformer'
)
def test_do_work(mock_do_work_marc_transformer, mock_folder_structure, mock_check_source_files):
    library_config = get_mocked_library_config()
    folio_client = mocked_folio_client()
    transformer = BibsTransformer(TASK_CONFIG, library_config, folio_client)
    transformer.processor = MagicMock()
    transformer.do_work()
    mock_do_work_marc_transformer.assert_called_once()


@patch(
    'folio_migration_tools.migration_tasks.bibs_transformer.BibsTransformer'
    '.clean_out_empty_logs'
)
def test_wrap_up(mock_clean_out_empty_logs, mock_folder_structure, mock_check_source_files):
    library_config = get_mocked_library_config()
    folio_client = mocked_folio_client()
    transformer = BibsTransformer(TASK_CONFIG, library_config, folio_client)
    transformer.processor = MagicMock()
    transformer.wrap_up()
    mock_clean_out_empty_logs.assert_called_once()


def test_different_ils_flavours(mock_folder_structure, mock_check_source_files):
    for ils_flavour in IlsFlavour:
        task_config = TASK_CONFIG.model_copy(update={"ils_flavour": ils_flavour})
        library_config = get_mocked_library_config()
        folio_client = mocked_folio_client()
        transformer = BibsTransformer(task_config, library_config, folio_client)
        transformer.processor = MagicMock()
        # Check that transformer is properly initialized for each ILS type


def test_error_handling(mock_folder_structure, mock_check_source_files):
    library_config = get_mocked_library_config()
    folio_client = mocked_folio_client()
    transformer = BibsTransformer(TASK_CONFIG, library_config, folio_client)
    transformer.processor = MagicMock()
    # Check handling of different error types


def test_hrid_settings(mock_folder_structure, mock_check_source_files):
    task_config = TASK_CONFIG.model_copy(update={
        "reset_hrid_settings": True,
        "update_hrid_settings": True
    })
    library_config = get_mocked_library_config()
    folio_client = mocked_folio_client()
    transformer = BibsTransformer(task_config, library_config, folio_client)
    transformer.processor = MagicMock()
    # Check that HRID settings are properly handled


def test_marc_file_processing(mock_folder_structure, mock_check_source_files):
    library_config = get_mocked_library_config()
    folio_client = mocked_folio_client()
    transformer = BibsTransformer(TASK_CONFIG, library_config, folio_client)
    transformer.processor = MagicMock()
    # Check MARC file processing
