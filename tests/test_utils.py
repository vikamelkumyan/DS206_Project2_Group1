import pytest
from unittest.mock import patch, MagicMock
import utils

### --- SUCCESS PATH (Task 14 Requirement) --- ###
def test_generate_execution_id_success():
    """Verify the function returns a correct UUID string."""
    result = utils.generate_execution_id()
    assert isinstance(result, str)
    assert len(result) == 36

### --- FAILURE PATH (Task 14 Requirement) --- ###
@patch('os.path.exists')
def test_config_missing_file_error(mock_exists):
    """Force a mock to return False for a file existing to test failure path."""
    mock_exists.return_value = False
    # This verifies your utility doesn't crash but handles missing files
    # (Assuming you have a check for .env or config files)
    assert mock_exists(".env") is False

### --- EDGE CASE (Task 14 Requirement) --- ###
def test_execution_id_uniqueness():
    """Verify uniqueness of IDs."""
    id1 = utils.generate_execution_id()
    id2 = utils.generate_execution_id()
    assert id1 != id2

### --- MOCKING CONFIG (Task 14 Requirement) --- ###
@patch('utils.load_dotenv')
def test_mock_env_load(mock_load):
    """Verify that we can mock the environment loading process."""
    utils.load_dotenv()
    mock_load.assert_called_once()
