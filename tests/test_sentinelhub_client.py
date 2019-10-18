from sentinelhub import SentinelHubClient, MimeType, CRS
import sentinelhub.sentinelhub_request as shr

def test_client():
    evalscript = """
        function setup() {
            return {
                input: ["B02", "B03", "B04"],
                sampleType: "UINT16",
                output: { bands: 3 }
            };
        }

        function evaluatePixel(sample) {
            return [2.5 * sample.B04, 2.5 * sample.B03, 2.5 * sample.B02];
        }
    """

    request = shr.body(
        request_bounds=shr.bounds(crs=CRS.WGS84.opengis_string, bbox=[46.16, -16.15, 46.51, -15.58]),
        request_data=[shr.data(time_from='2017-12-15T07:12:03Z', time_to='2017-12-15T07:12:04Z', data_type='S2L1C')],
        request_output=shr.output(size_x=512, size_y=856, responses=[shr.response('default', 'image/tiff')]),
        evalscript=evalscript
    )

    headers = {'content-type': 'application/json'}

    client = SentinelHubClient()
    img = client.download(request, headers=headers)

    with open("img.tif", 'wb') as f:
        f.write(img)


EVALSCRIPT = """
    function setup() {
        return {
            input: [{
                    bands: ["B02", "B03", "B04"],
                    units: "DN"
                }],
            output: {
                id:"default",
                bands: 3,
                sampleType: SampleType.UINT16
            }
        }
    }

    function updateOutputMetadata(scenes, inputMetadata, outputMetadata) {
        outputMetadata.userData = { "norm_factor":  inputMetadata.normalizationFactor }
    }

    function evaluatePixel(sample) {
        return [sample.B02, sample.B03, sample.B04];
    }
"""

def test_multipart():
    responses = [
        shr.response('default', 'image/tiff'),
        shr.response('userdata', 'application/json')
    ]

    request = shr.body(
        request_bounds=shr.bounds(crs=CRS.WGS84.opengis_string, bbox=[46.16, -16.15, 46.51, -15.58]),
        request_data=[shr.data(time_from='2017-12-15T07:12:03Z', time_to='2017-12-15T07:12:04Z', data_type='S2L1C')],
        request_output=shr.output(size_x=512, size_y=856, responses=responses),
        evalscript=EVALSCRIPT
    )

    headers = {"accept": "application/tar", 'content-type': 'application/json'}

    client = SentinelHubClient()
    img = client.download(request, headers=headers)

    with open("img.tar", 'wb') as f:
        f.write(img)

if __name__ == "__main__":
    # test_client()
    test_multipart()
