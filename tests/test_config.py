import unittest
from unittest.mock import patch

import config


class LinearTeamKeysTest(unittest.TestCase):
    def test_defaults_to_the_primary_team(self):
        with patch.object(config, "load_config", return_value={"linear_team_key": "APO"}):
            self.assertEqual(config.get_linear_team_keys(), ["APO"])

    def test_extends_primary_team_without_duplicates(self):
        loaded = {"linear_team_key": "APO", "linear_team_keys": ["SUP", "APO", " ", "SUP"]}
        with patch.object(config, "load_config", return_value=loaded):
            self.assertEqual(config.get_linear_team_keys(), ["APO", "SUP"])

    def test_accepts_a_single_string(self):
        loaded = {"linear_team_key": "APO", "linear_team_keys": "SUP"}
        with patch.object(config, "load_config", return_value=loaded):
            self.assertEqual(config.get_linear_team_keys(), ["APO", "SUP"])


class BugLabelNamesTest(unittest.TestCase):
    def test_defaults_to_bug(self):
        with patch.object(config, "load_config", return_value={}):
            self.assertEqual(config.get_bug_label_names(), ["Bug"])

    def test_uses_configured_labels(self):
        with patch.object(config, "load_config", return_value={"bug_labels": ["Bug", "Issue"]}):
            self.assertEqual(config.get_bug_label_names(), ["Bug", "Issue"])

    def test_ignores_blank_entries_and_falls_back(self):
        with patch.object(config, "load_config", return_value={"bug_labels": [" ", None]}):
            self.assertEqual(config.get_bug_label_names(), ["Bug"])


if __name__ == "__main__":
    unittest.main()
