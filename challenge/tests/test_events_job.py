import json
import os
import shutil

from tests import PySparkTest
import events_job


class EventsJobTest(PySparkTest):

    def setUp(self):
        """
        Set up some sample data that can be used in the tests methods
        """
        self.sample_data_path = '/tmp/sample_data.json'
        self.sample_data = [
            {
                'event_id': '450e133e-14e1-4f4c-8bbb-993d4d56e3ca',
                'timestamp': '2007-12-10T04:11:25',
                'domain': 'account',
                'event_type': 'status-detail',
                'data': {
                    'id': 8327
                }
            },
            {
                'event_id': 'a58de256-a118-4ec8-b3f5-ef5e5ace5063',
                'timestamp': '2000-06-02T23:02:05',
                'domain': 'transaction',
                'event_type': 'status-new',
                'data': {
                    'id': 2316
                }
            },
            {
                'event_id': '7920032b-a6f3-486e-ac08-14e4df1d0495',
                'timestamp': '2017-09-17T22:05:36',
                'domain': 'transaction',
                'event_type': 'status-detail',
                'data': {
                    'id': 386
                }
            }
        ]
        EventsJobTest._write_sample_file(
            self.sample_data, self.sample_data_path
        )

    def tearDown(self):
        """
        Deletes the sample data provisioned for testing
        """
        os.remove(self.sample_data_path)

    @staticmethod
    def _write_sample_file(sample_data, output_path):
        with open(output_path, 'w') as file:
            file.write(json.dumps(sample_data))

    def test_should_dedup_events_by_timestamp(self):
        """
        Test method to validate that duplicated events are cleaned in the dataset
        """
        duplicated_event_id = '450e133e-14e1-4f4c-8bbb-993d4d56e3ca'
        newer_timestamp = '2021-02-10T04:11:25'
        sample_df = self.spark.createDataFrame(
            [
                (duplicated_event_id, '2007-12-10T04:11:25', 'account', 'status-detail', '{ "id": 8327 }'),
                (duplicated_event_id, newer_timestamp, 'account', 'status-detail', '{ "id": 8328 }'),
                ('a58de256-a118-4ec8-b3f5-ef5e5ace5063', '2017-09-17T22:05:36', 'transaction', 'status-new', '{ "id": 8329 }'),
                ('2cc5123a-7c38-41e8-bd50-a839cd0c4e44', '2007-12-10T04:11:25', 'account', 'status-detail', '{ "id": 8330 }'),
            ],
            ['event_id', 'timestamp', 'domain', 'event_type', 'data']
        )
        output_df = events_job.dedup_events(sample_df)
        assert output_df.count() == 3
        assert output_df.filter(
            f'event_id == "{duplicated_event_id}" and timestamp == "{newer_timestamp}"'
        ).count() == 1

    def test_should_read_events_file(self):
        """
        Test method to validate that the correct dataset is being read
        """
        events_df = events_job.read_events_data(self.sample_data_path)

        assert events_df.count() == len(self.sample_data)

        for event in self.sample_data:
            assert events_df.filter(
                f'event_id == \'{event["event_id"]}\' and timestamp == \'{event["timestamp"]}\''
            ).count() == 1

    def test_should_write_events_file(self):
        """
        Test method to validate that the parquet events file is being
        written accordingly
        """
        events_output_path = '/tmp/output-events'
        try:
            events_df = events_job.read_events_data(self.sample_data_path)
            events_job.trigger_events_processing(events_df, events_output_path)

            output_events_df = self.spark.read.parquet(events_output_path)

            assert output_events_df.select(*events_df.columns).exceptAll(events_df).count() == 0

            output_directories = os.listdir(events_output_path)
            for domain in ['domain=account', 'domain=transaction']:
                assert domain in output_directories
                for directory in os.listdir(f'{events_output_path}/{domain}'):
                    assert 'event_type=' in directory or '_SUCCESS' in directory
        finally:
            shutil.rmtree(events_output_path)
