import unittest
import datetime
import numpy as np

from shapely.geometry import MultiPolygon

from sentinelhub import WmsRequest, WcsRequest, CRS, MimeType, CustomUrlParam, ServiceType, DataSource, BBox,\
    WebFeatureService, DownloadFailedException, TestSentinelHub, TestCaseContainer
from sentinelhub.data_request import OgcRequest
from sentinelhub.ogc import OgcImageService


class TestOgc(TestSentinelHub):

    class OgcTestCase(TestCaseContainer):
        """
        Container for each test case of sentinelhub OGC functionalities
        """
        def __init__(self, name, request, result_len, save_data=False, **kwargs):
            super().__init__(name, request, **kwargs)

            self.result_len = result_len
            self.save_data = save_data

            self.data = None

        def collect_data(self):
            if self.save_data:
                self.request.save_data(redownload=True, data_filter=self.data_filter)
                self.data = self.request.get_data(save_data=True, data_filter=self.data_filter)
            else:
                self.data = self.request.get_data(redownload=True, data_filter=self.data_filter)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        wgs84_bbox = BBox(bbox=(-5.23, 48.0, -5.03, 48.17), crs=CRS.WGS84)
        wgs84_bbox_2 = BBox(bbox=(21.3, 64.0, 22.0, 64.5), crs=CRS.WGS84)
        wgs84_bbox_3 = BBox(bbox=(-72.0, -70.4, -71.8, -70.2), crs=CRS.WGS84)
        wgs84_bbox_4 = BBox(bbox=(-72.0, -66.4, -71.8, -66.2), crs=CRS.WGS84)
        wgs84_bbox_byoc = BBox(bbox=(13.82387, 45.85221, 13.83313, 45.85901), crs=CRS.WGS84)
        pop_web_bbox = BBox(bbox=(1292344.0, 5195920.0, 1310615.0, 5214191.0), crs=CRS.POP_WEB)
        geometry_wkt_pop_web = 'POLYGON((1292344.0 5205055.5, 1301479.5 5195920.0, 1310615.0 5205055.5, ' \
                               '1301479.5 5214191.0, 1292344.0 5205055.5))'
        geometry_wkt_wgs84 = 'POLYGON((-5.13 48, -5.23 48.09, -5.13 48.17, -5.03 48.08, -5.13 48))'
        img_width = 100
        img_height = 100
        resx = '53m'
        resy = '78m'
        expected_date = datetime.datetime.strptime('2017-10-07T11:20:58', '%Y-%m-%dT%H:%M:%S')

        cls.test_cases = [
            cls.OgcTestCase('generalWmsTest',
                            OgcRequest(data_folder=cls.OUTPUT_FOLDER, image_format=MimeType.TIFF_d32f, bbox=wgs84_bbox,
                                       layer='BANDS-S2-L1C', maxcc=0.5, size_x=img_width, size_y=img_height,
                                       time=(datetime.date(year=2017, month=1, day=5),
                                             datetime.date(year=2017, month=12, day=16)),
                                       service_type=ServiceType.WMS, time_difference=datetime.timedelta(days=10)),
                            result_len=14, img_min=0.0, img_max=1.5964, img_mean=0.1810, img_median=0.1140, tile_num=29,
                            save_data=True, data_filter=[0, -2, 0]),
            cls.OgcTestCase('generalWcsTest',
                            OgcRequest(data_folder=cls.OUTPUT_FOLDER, image_format=MimeType.TIFF_d32f, bbox=wgs84_bbox,
                                       layer='BANDS-S2-L1C', maxcc=0.6, size_x=resx, size_y=resy,
                                       time=(datetime.datetime(year=2017, month=10, day=7, hour=1),
                                             datetime.datetime(year=2017, month=12, day=11)),
                                       service_type=ServiceType.WCS, time_difference=datetime.timedelta(hours=1)),
                            result_len=4, img_min=0.0002, img_max=0.5266, img_mean=0.1038, img_median=0.0948,
                            tile_num=6, date_check=expected_date, save_data=True, data_filter=[0, -1]),
            # CustomUrlParam tests:
            cls.OgcTestCase('customUrlAtmcorQualitySampling',
                            WmsRequest(data_folder=cls.OUTPUT_FOLDER, image_format=MimeType.PNG,
                                       layer='TRUE-COLOR-S2-L1C', width=img_width, bbox=wgs84_bbox,
                                       time=('2017-10-01', '2017-10-02'),
                                       custom_url_params={CustomUrlParam.SHOWLOGO: True,
                                                          CustomUrlParam.ATMFILTER: 'ATMCOR',
                                                          CustomUrlParam.QUALITY: 100,
                                                          CustomUrlParam.DOWNSAMPLING: 'BICUBIC',
                                                          CustomUrlParam.UPSAMPLING: 'BICUBIC'}),
                            result_len=1, img_min=12, img_max=255, img_mean=194.247556, img_median=206, tile_num=2,
                            data_filter=[0, -1]),
            cls.OgcTestCase('customUrlPreview',
                            WmsRequest(data_folder=cls.OUTPUT_FOLDER, image_format=MimeType.PNG,
                                       layer='TRUE-COLOR-S2-L1C', height=img_height, bbox=wgs84_bbox,
                                       time=('2017-10-01', '2017-10-02'),
                                       custom_url_params={CustomUrlParam.SHOWLOGO: True,
                                                          CustomUrlParam.PREVIEW: 2}),
                            result_len=1, img_min=27, img_max=253, img_mean=177.17819, img_median=178, tile_num=2),
            cls.OgcTestCase('customUrlEvalscripturl',
                            WcsRequest(data_folder=cls.OUTPUT_FOLDER, image_format=MimeType.PNG,
                                       layer='TRUE-COLOR-S2-L1C', resx=resx, resy=resy, bbox=pop_web_bbox,
                                       time=('2017-10-01', '2017-10-02'),
                                       custom_url_params={CustomUrlParam.SHOWLOGO: True,
                                                          CustomUrlParam.EVALSCRIPTURL:
                                                              'https://raw.githubusercontent.com/sentinel-hub/'
                                                              'customScripts/master/sentinel-2/false_color_infrared/'
                                                              'script.js'}),
                            result_len=1, img_min=42, img_max=255, img_mean=230.7199, img_median=255, tile_num=3),
            cls.OgcTestCase('customUrlEvalscript,Transparent,Geometry',
                            WcsRequest(data_folder=cls.OUTPUT_FOLDER, image_format=MimeType.PNG,
                                       layer='TRUE-COLOR-S2-L1C', resx=resx, resy=resy, bbox=wgs84_bbox,
                                       time=('2017-10-01', '2017-10-02'),
                                       custom_url_params={CustomUrlParam.SHOWLOGO: True,
                                                          CustomUrlParam.TRANSPARENT: True,
                                                          CustomUrlParam.EVALSCRIPT: 'return [B10,B8A, B03 ]',
                                                          CustomUrlParam.GEOMETRY: geometry_wkt_wgs84}),
                            result_len=1, img_min=0, img_max=255, img_mean=54.3482, img_median=1.0, tile_num=2),
            cls.OgcTestCase('FalseLogo,BgColor,Geometry',
                            WmsRequest(data_folder=cls.OUTPUT_FOLDER, image_format=MimeType.PNG,
                                       layer='TRUE-COLOR-S2-L1C', width=img_width, height=img_height, bbox=pop_web_bbox,
                                       time=('2017-10-01', '2017-10-02'),
                                       custom_url_params={CustomUrlParam.SHOWLOGO: False,
                                                          CustomUrlParam.BGCOLOR: "F4F86A",
                                                          CustomUrlParam.GEOMETRY: geometry_wkt_pop_web}),
                            result_len=1, img_min=64, img_max=MimeType.PNG.get_expected_max_value(), img_mean=213.6124,
                            img_median=242.0, tile_num=3),
            # DataSource tests:
            cls.OgcTestCase('S2 L1C Test',
                            WmsRequest(data_source=DataSource.SENTINEL2_L1C, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d8, layer='BANDS-S2-L1C',
                                       width=img_width, height=img_height, bbox=wgs84_bbox,
                                       time=('2017-10-01', '2017-10-02')),
                            result_len=1, img_min=0, img_max=160, img_mean=60.50514, img_median=63.0,
                            tile_num=2),
            cls.OgcTestCase('S2 L2A Test',
                            WmsRequest(data_source=DataSource.SENTINEL2_L2A, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF, layer='BANDS-S2-L2A',
                                       width=img_width, height=img_height, bbox=wgs84_bbox,
                                       time=('2017-10-01', '2017-10-02')),
                            result_len=1, img_min=0.0, img_max=MimeType.TIFF.get_expected_max_value(),
                            img_mean=22744.01498, img_median=21391.0, tile_num=2),
            cls.OgcTestCase('L8 Test',
                            WmsRequest(data_source=DataSource.LANDSAT8, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-L8',
                                       width=img_width, height=img_height, bbox=wgs84_bbox,
                                       time=('2017-10-05', '2017-10-10'), time_difference=datetime.timedelta(hours=1)),
                            result_len=1, img_min=0.0012, img_max=285.78003, img_mean=52.06088, img_median=0.5192,
                            tile_num=2),
            cls.OgcTestCase('DEM Test',
                            WmsRequest(data_source=DataSource.DEM, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='DEM',
                                       width=img_width, height=img_height, bbox=wgs84_bbox),
                            result_len=1, img_min=-108.0, img_max=-18.0, img_mean=-72.2097, img_median=-72.0),
            cls.OgcTestCase('MODIS Test',
                            WmsRequest(data_source=DataSource.MODIS, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-MODIS',
                                       width=img_width, height=img_height, bbox=wgs84_bbox, time='2017-10-01'),
                            result_len=1, img_min=0.0, img_max=3.2767, img_mean=0.136548, img_median=0.00240,
                            tile_num=1),
            cls.OgcTestCase('S1 IW Test',
                            WmsRequest(data_source=DataSource.SENTINEL1_IW, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-S1-IW',
                                       width=img_width, height=img_height, bbox=wgs84_bbox,
                                       time=('2017-10-01', '2017-10-02'),
                                       time_difference=datetime.timedelta(hours=1)),
                            result_len=1, img_min=0.00150, img_max=MimeType.TIFF_d32f.get_expected_max_value(),
                            img_mean=0.1135815, img_median=0.06210, tile_num=2),
            cls.OgcTestCase('S1 EW Test',
                            WmsRequest(data_source=DataSource.SENTINEL1_EW, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-S1-EW',
                                       width=img_width, height=img_height, bbox=wgs84_bbox_2,
                                       time=('2018-2-7', '2018-2-8'),
                                       time_difference=datetime.timedelta(hours=1)),
                            result_len=2, img_min=0.0003, img_max=1.0, img_mean=0.53118, img_median=1.0, tile_num=3),
            cls.OgcTestCase('S1 EW SH Test',
                            WmsRequest(data_source=DataSource.SENTINEL1_EW_SH,
                                       data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d16, layer='BANDS-S1-EW-SH',
                                       width=img_width, height=img_height, bbox=wgs84_bbox_3,
                                       custom_url_params={CustomUrlParam.SHOWLOGO: True}, time=('2018-2-6', '2018-2-8'),
                                       time_difference=datetime.timedelta(hours=1)),
                            result_len=1, img_min=472, img_max=59288, img_mean=5324.8715, img_median=944.0,
                            tile_num=1),
            cls.OgcTestCase('S1 IW ASC Test',
                            WmsRequest(data_source=DataSource.SENTINEL1_IW_ASC, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-S1-IW',
                                       width=img_width, height=img_height, bbox=wgs84_bbox,
                                       time=('2017-10-01', '2017-10-03'),
                                       time_difference=datetime.timedelta(hours=1)),
                            result_len=1, img_min=0.00150, img_max=MimeType.TIFF_d32f.get_expected_max_value(),
                            img_mean=0.1135815, img_median=0.0621000, tile_num=2),
            cls.OgcTestCase('S1 EW ASC Test',
                            WmsRequest(data_source=DataSource.SENTINEL1_EW_ASC, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-S1-EW',
                                       width=img_width, height=img_height, bbox=wgs84_bbox_2,
                                       time=('2018-2-7', '2018-2-8'),
                                       time_difference=datetime.timedelta(hours=1)),
                            result_len=1, img_min=0.0003, img_max=0.2322, img_mean=0.02199, img_median=0.0102,
                            tile_num=2),
            cls.OgcTestCase('S1 EW SH ASC Test',
                            WmsRequest(data_source=DataSource.SENTINEL1_EW_SH_ASC,
                                       data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d16, layer='BANDS-S1-EW-SH',
                                       width=img_width, height=img_height, bbox=wgs84_bbox_3,
                                       custom_url_params={CustomUrlParam.SHOWLOGO: True}, time=('2018-2-6', '2018-2-8'),
                                       time_difference=datetime.timedelta(hours=1)),
                            result_len=1, img_min=472, img_max=59288, img_mean=5324.8715, img_median=944.0,
                            tile_num=1),
            cls.OgcTestCase('S1 IW DES Test',
                            WmsRequest(data_source=DataSource.SENTINEL1_IW_DES, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-S1-IW',
                                       width=img_width, height=img_height, bbox=wgs84_bbox,
                                       time=('2017-10-01', '2017-10-05'),
                                       time_difference=datetime.timedelta(hours=1)),
                            result_len=1, img_min=0.0, img_max=0.07700,
                            img_mean=0.0210801, img_median=0.0132, tile_num=1),
            cls.OgcTestCase('S1 EW DES Test',
                            WmsRequest(data_source=DataSource.SENTINEL1_EW_DES, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-S1-EW',
                                       width=img_width, height=img_height, bbox=wgs84_bbox_2,
                                       time=('2018-2-7', '2018-2-8'),
                                       time_difference=datetime.timedelta(hours=1)),
                            result_len=1, img_min=0.0003, img_max=1.0, img_mean=0.53118, img_median=1.0, tile_num=1),
            cls.OgcTestCase('S1 EW SH DES Test',
                            WmsRequest(data_source=DataSource.SENTINEL1_EW_SH_DES,
                                       data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d16, layer='BANDS-S1-EW-SH',
                                       width=img_width, height=img_height, bbox=wgs84_bbox_4,
                                       custom_url_params={CustomUrlParam.SHOWLOGO: True}, time=('2018-2-5', '2018-2-6'),
                                       time_difference=datetime.timedelta(hours=1)),
                            result_len=1, img_min=3978, img_max=61495, img_mean=18372.9765, img_median=15171.0,
                            tile_num=1),
            # cls.OgcTestCase('BYOC Test',
            #                 WmsRequest(data_source=DataSource('31df1de4-8bd4-43e0-8c3f-b04262d111b6'),
            #                            data_folder=cls.OUTPUT_FOLDER,
            #                            image_format=MimeType.TIFF_d16, layer='DEMO_BYOC_LAYER',
            #                            width=img_width, height=img_height, bbox=wgs84_bbox_byoc,
            #                            custom_url_params={CustomUrlParam.SHOWLOGO: False}),
            #                 result_len=1, img_min=0, img_max=65535, img_mean=17158.8938, img_median=15728.0,
            #                 tile_num=1)
        ]
        """
        # Test case for eocloud data source
        cls.test_cases.extend([
            cls.OgcTestCase('EOCloud S1 IW Test',
                            WmsRequest(data_source=DataSource.SENTINEL1_IW, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS_S1_IW',
                                       width=img_width, height=img_height, bbox=wgs84_bbox,
                                       time=('2017-10-01', '2017-10-02'),
                                       time_difference=datetime.timedelta(hours=1)),
                            result_len=1, img_min=0.0, img_max=0.49706, img_mean=0.04082, img_median=0.00607,
                            tile_num=2),
        ])
        """

        for test_case in cls.test_cases:
            test_case.collect_data()

    def test_return_type(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                self.assertTrue(isinstance(test_case.data, list), "Expected a list")
                result_len = test_case.result_len if test_case.data_filter is None else len(test_case.data_filter)
                self.assertEqual(len(test_case.data), result_len,
                                 "Expected a list of length {}, got length {}".format(result_len, len(test_case.data)))

    def test_wfs(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                tile_iter = test_case.request.get_tiles()
                tile_n = len(list(tile_iter)) if tile_iter else None

                self.assertEqual(tile_n, test_case.tile_num,
                                 "Expected {} tiles, got {}".format(test_case.tile_num, tile_n))

    def test_download_url(self):
        for test_case in self.test_cases:
            if test_case.url_check is not None:
                with self.subTest(msg='Test case {}'.format(test_case.name)):
                    download_url = self.request.get_url_list()[0]
                    self.assertTrue(test_case.url_check in download_url,
                                    "Parameter '{}' not in download url {}.".format(test_case.url_check, download_url))

    def test_get_dates(self):
        for test_case in self.test_cases:
            if test_case.date_check is not None:
                with self.subTest(msg='Test case {}'.format(test_case.name)):
                    dates = OgcImageService().get_dates(test_case.request)
                    self.assertEqual(len(dates), test_case.result_len,
                                     msg="Expected {} dates, got {}".format(test_case.result_len, len(dates)))
                    self.assertEqual(test_case.date_check, dates[0],
                                     msg="Expected date {}, got {}".format(test_case.date_check, dates[0]))

    def test_filter(self):
        for test_case in self.test_cases:
            if test_case.data_filter is not None:
                with self.subTest(msg='Test case {}'.format(test_case.name)):
                    if (test_case.data_filter[0] - test_case.data_filter[-1]) % test_case.result_len == 0:
                        self.assertTrue(np.array_equal(test_case.data[0], test_case.data[-1]),
                                        msg="Expected first and last output to be equal, got different")
                    else:
                        self.assertFalse(np.array_equal(test_case.data[0], test_case.data[-1]),
                                         msg="Expected first and last output to be different, got the same")

    def test_stats(self):
        for test_case in self.test_cases:
            self.test_numpy_data(test_case.data[0], exp_min=test_case.img_min, exp_max=test_case.img_max,
                                 exp_mean=test_case.img_mean, exp_median=test_case.img_median,
                                 test_name=test_case.name)

    def test_to_large_request(self):
        bbox = BBox((-5.23, 48.0, -5.03, 48.17), CRS.WGS84)
        request = WmsRequest(layer='TRUE-COLOR-S2-L1C', height=6000, width=6000, bbox=bbox,
                             time=('2017-10-01', '2017-10-02'))

        with self.assertRaises(DownloadFailedException):
            request.get_data()


class TestWebFeatureService(TestSentinelHub):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        wgs84_bbox = BBox(bbox=(-5.23, 48.0, -5.03, 48.17), crs=CRS.WGS84)
        time_interval = (datetime.date(year=2017, month=1, day=5), datetime.date(year=2017, month=12, day=16))

        cls.test_cases = [
            TestCaseContainer('General test',
                              WebFeatureService(wgs84_bbox, time_interval, data_source=DataSource.SENTINEL2_L1C,
                                                maxcc=0.1),
                              result_len=13),
            TestCaseContainer('Latest date only',
                              WebFeatureService(wgs84_bbox, 'latest', data_source=DataSource.SENTINEL2_L2A),
                              result_len=1),
        ]

    def test_results(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                iterator = test_case.request

                features = list(iterator)
                dates = iterator.get_dates()
                geometries = iterator.get_geometries()
                tiles = iterator.get_tiles()

                for result_list, expected_type in [(features, dict), (dates, datetime.datetime),
                                                   (geometries, MultiPolygon), (tiles, tuple)]:
                    self.assertEqual(len(result_list), test_case.result_len)
                    for result in result_list:
                        self.assertTrue(isinstance(result, expected_type),
                                        msg='Expected type {}, got type {}'.format(expected_type, type(result)))


if __name__ == '__main__':
    unittest.main()
