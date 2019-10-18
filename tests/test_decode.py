import unittest
import os

import numpy as np

from sentinelhub import CRS, MimeType, TestSentinelHub
from sentinelhub.decoding import decode_tar


class TestDecode(TestSentinelHub):
    def test_tar(self):
        tar_path = os.path.join(self.INPUT_FOLDER, 'img.tar')
        with open(tar_path, 'rb') as tar_file:
            tar_bytes = tar_file.read()

        image, metadata = decode_tar(tar_bytes)

        self.assertIsInstance(image, np.ndarray)
        self.assertEqual(image.shape, (856, 512, 3))

        self.assertIn('norm_factor', metadata)
        self.assertEqual(metadata['norm_factor'], 0.0001)


if __name__ == "__main__":
    unittest.main()
