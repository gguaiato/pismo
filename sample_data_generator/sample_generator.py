from argparse import ArgumentParser
from faker import Faker

import json
import os
import random
import uuid

ACCOUNT_DOMAIN = 'account'
TRANSACTION_DOMAIN = 'transaction'
MIN_EVENT_FILES = 3

DOMAIN_TYPES = [ACCOUNT_DOMAIN, TRANSACTION_DOMAIN]
EVENT_TYPES = [
    'status-change',
    'status-new',
    'status-detail'
]

fake = Faker(['pt_BR', 'en_US'])


def _save_events_file(events, events_files_dir, events_output_file):
    if not os.path.exists(events_files_dir):
        os.makedirs(events_files_dir)

    with open(f'{events_files_dir}/{events_output_file}', 'w') as file:
        file.write(json.dumps(events))


def _generate_sample_event():
    event_id = str(uuid.uuid4())
    timestamp = fake.date_time().strftime('%Y-%m-%dT%H:%M:%S')
    data_id = fake.pyint()
    return {
        'event_id': event_id,
        'timestamp': timestamp,
        'domain': random.choice(DOMAIN_TYPES),
        'event_type': random.choice(EVENT_TYPES),
        'data': {
            'id': data_id
        }
    }


def generate_sample_files(events_files_path, event_files, max_events_file):
    for file_index in range(event_files):
        events = [_generate_sample_event() for i in range(max_events_file)]
        events_output_file = f'events-{str(uuid.uuid4())[:8]}.json'
        _save_events_file(events, events_files_path, events_output_file)


if __name__ == '__main__':
    args = ArgumentParser(
        description='Generates sample files to Pismo Spark Job'
    )
    args.add_argument(
        '--event_files',
        help='The quantity of event files that should be generated',
        type=int,
        default=3
    )
    args.add_argument(
        '--events_per_file',
        help='Quantity of events that should be generated in each file',
        type=int,
        default=5
    )
    args.add_argument(
        '--output_dir',
        help='The directory where the files will be written to',
        type=str
    )
    parsed = args.parse_args()
    generate_sample_files(
        parsed.output_dir, parsed.event_files, parsed.events_per_file
    )
