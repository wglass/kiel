import unittest

from kiel import constants


class ConstantsTests(unittest.TestCase):

    def test_all_error_codes_accounted_for(self):
        highest_known_error_code = 16
        unused_error_codes = {13}

        for code in range(-1, highest_known_error_code + 1):
            if code in unused_error_codes:
                continue

            self.assertIn(code, constants.ERROR_CODES.keys())
