import unittest

from sentinelhub import Session


REQUEST_BODY = {
    "input": {
        "bounds": {
            "bbox": [
                13.822174072265625,
                45.85080395917834,
                14.55963134765625,
                46.29191774991382
            ],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            14.000701904296873,
                            46.23685258143992
                        ],
                        [
                            13.822174072265625,
                            46.09037664604301
                        ],
                        [
                            14.113311767578125,
                            45.85080395917834
                        ],
                        [
                            14.55963134765625,
                            46.038922598236
                        ],
                        [
                            14.441528320312498,
                            46.28717293114449
                        ],
                        [
                            14.17236328125,
                            46.29191774991382
                        ],
                        [
                            14.000701904296873,
                            46.23685258143992
                        ]
                    ]
                ]
            },
            "properties": {
                "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
            }
        },
        "data": [
            {
                "dataFilter": {
                    "timeRange": {
                        "from": "2018-10-01T00:00:00.000Z",
                        "to": "2018-11-01T00:00:00.000Z"
                    }
                },
                "processing": {
                    "upsampling": "NEAREST",
                    "downsampling": "NEAREST"
                },
                "type": "S2L1C"
            }
        ]
    },
    "output": {
        "width": 512,
        "height": 512,
        "responses": "png response object with \"default\" identifier"
    },
}


class TestSession(unittest.TestCase):
    def test_session(self):
        session = Session()

        resp = session.post(REQUEST_BODY)

        print(resp)

if __name__ == '__main__':
    unittest.main()
