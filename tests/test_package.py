"""Basic tests for the qa_logistic_care_costs project."""


def test_project_structure():
    """Test that basic project structure exists."""
    import os

    # Test that key directories exist
    assert os.path.exists("src"), "src directory should exist"
    assert os.path.exists("notebooks"), "notebooks directory should exist"
    assert os.path.exists("tests"), "tests directory should exist"
    assert os.path.exists("docs"), "docs directory should exist"


def test_requirements_files():
    """Test that requirements files exist."""
    import os

    assert os.path.exists("requirements.txt"), "requirements.txt should exist"
    assert os.path.exists("dev-requirements.txt"), "dev-requirements.txt should exist"


def test_makefile_exists():
    """Test that Makefile exists."""
    import os

    assert os.path.exists("Makefile"), "Makefile should exist"
