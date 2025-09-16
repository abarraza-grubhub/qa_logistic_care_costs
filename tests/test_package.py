"""Basic tests for the qa_logistic_care_costs package."""

from qa_logistic_care_costs import __version__, __author__, __description__


def test_package_metadata():
    """Test that package metadata is properly defined."""
    assert __version__ == "0.1.0"
    assert __author__ == "Grubhub QA Team"
    assert __description__ == "Analytics and modeling for QA logistic care costs"


def test_package_import():
    """Test that the package can be imported successfully."""
    import qa_logistic_care_costs

    assert qa_logistic_care_costs is not None
