import boto3 as aws
import threading
import os
import errno
import re
import datetime
import time
import ConfigParser


class FirehoseSender:
    def __init__(self, access_key, secret_key, stream_name, max_rows=10, remove_sent=False, json=True):
        # type: (basestring, basestring, basestring, int, bool, bool) -> None
        # Initialize firehose client with access and secret key
        self.access_key = access_key
        self.secret_key = secret_key  # Private streams need this
        self.stream_name = stream_name  # Firehose stream name
        self.actual_file = None
        self.max_rows = max_rows  # number of rows needed to set a file to 'ready'
        self.running = True
        self.remove_sent = remove_sent  # if true it will remove sent files
        self.json = json  # Will it send JSON files?

        self.debug = True  # Hard-coded: True enables all debug print statements

        self.client = aws.client('firehose', region_name='eu-west-1',  aws_access_key_id=access_key,
                                 aws_secret_access_key=secret_key)
        self._stop = threading.Event()
        self._thread = None
        self.validate_stream()
        self.check_files()

    @classmethod
    def from_config(cls, file_name):
        # type: (basestring) -> FirehoseSender
        # Create a FirehoseSender taking parameters from config file
        # If there is a problem with config it will raise an exception that, if not handled, stop execution
        config = ConfigParser.SafeConfigParser()
        config.read(file_name)

        access_key = config.get('Client', 'access_key')
        secret_key = config.get('Client', 'secret_key')
        stream_name = config.get('Client', 'stream_name')
        num_rows = config.getint('Client', 'num_rows')
        remove_sent = config.getboolean('Client', 'remove_sent')
        json = config.getboolean('Client', 'json')

        return cls(access_key, secret_key, stream_name, num_rows, remove_sent, json)

    def validate_stream(self):
        # Check if the stream is active
        stream = self.client.describe_delivery_stream(DeliveryStreamName=self.stream_name)
        if 'ACTIVE' != stream['DeliveryStreamDescription']['DeliveryStreamStatus']:
            raise ValueError("{} is not active".format(self.stream_name))

    def start(self):
        # Starts monitor thread
        self._thread = threading.Thread(target=self.monitor)
        self._thread.setDaemon(True)
        self._thread.start()

    def stop(self):
        # Stops monitor thread
        self._stop.set()
        self.running = False
        self._thread.join()
        self._thread = None

    def prepare_json(self, element):
        # Convert a dict into a JSON string
        assert isinstance(element, dict)
        output = "{"
        for key in element:
            if isinstance(element[key], basestring):
                output = output + "\"" + key + "\": " + "\"" + element[key] + "\", "
            else:
                output = output + "\"" + key + "\": " + str(element[key]) + ", "
        output = output[:-2] + "}"
        return output

    def write_element(self, element):
        # Write element in last tmp file
        opened_file = open("out/tmp_out/" + self.actual_file, "a+")
        opened_file.write(element + "\n")
        opened_file.close()
        self.check_files()

    @staticmethod
    def make_dir(directory):
        # Create a path if not existing
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    @staticmethod
    def file_len(file_name):
        # Counts number of lines in a file
        i = 0
        with open(file_name) as f:
            for i, l in enumerate(f):
                pass
        return i + 1

    def check_files(self):
        # Move files that are ready to be sent in the relative directory
        self.make_dir("out")
        self.make_dir("out/tmp_out")
        self.make_dir("out/ready_out")

        uncompleted_file = None
        for file_name in os.listdir("out/tmp_out"):
            if re.match("out[0-9]{17}", file_name) is None:  # Check if the file is valid
                continue
            num_rows = self.file_len("out/tmp_out/" + file_name)
            if num_rows < self.max_rows:
                uncompleted_file = file_name
            else:
                print "File " + file_name + " ready to be sent."
                os.rename("out/tmp_out/" + file_name, "out/ready_out/" + file_name)
        if uncompleted_file is None:
            self.actual_file = "out" + datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S%f')[:-3]
        else:
            self.actual_file = uncompleted_file

    def check_integrity(self, text):
        # Check integrity of JSON files
        # Warning:  values or keys with escaped curly brackets will be seen as invalid and not sent
        #           if you need escaped curly brackets you can change the following regex or set json to false
        #           in the constructor (it will disable the use of this method)
        par = re.findall(r"\{[^{}]+\}", text)
        str_int = '\n'.join(par) + '\n'
        return str_int

    def monitor(self):
        # Check files in ready_out dir and send them to firehose stream
        if self.debug:
            print "Entering thread.."
        while self.running:
            self.make_dir("out")
            self.make_dir("out/tmp_out")
            self.make_dir("out/ready_out")
            if not self.remove_sent:
                self.make_dir("out/sent_out")
            for file_name in os.listdir("out/ready_out"):
                if re.match("out[0-9]{17}", file_name) is None:  # Check if the file is valid
                    continue
                with file("out/ready_out/" + file_name) as f:  # Read all file
                    text = f.read()
                if text is None:
                    continue
                if self.json:
                    text = self.check_integrity(text)  # integrity check (for files unexpectedly closed and corrupted)
                try:
                    response = self.client.put_record(
                            DeliveryStreamName=self.stream_name,
                            Record={
                                'Data': text
                            })
                    if response['RecordId'] is not None:
                        if self.debug:
                            print "File '" + file_name + "' successfully sent! RecordId: " + response['RecordId']
                        if self.remove_sent:
                            os.remove("out/ready_out/" + file_name)
                        else:
                            os.rename("out/ready_out/" + file_name, "out/sent_out/" + file_name)
                    else:  # This should not happen
                        print "Failed putting record. Data: " + text
                except Exception as e:
                    print "Failed putting record. Exception: " + format(e)
            time.sleep(10)


def main():
    # Test - Example of use
    fhs = FirehoseSender.from_config('firehose.conf')  # Takes client configuration from file
    for i in range(10):  # Write a file that is ready to be sent
        fhs.write_element('{"id": 1, "some_key": "some_value", "acqua": "water", "datetime":'
                          ' "2018-06-21 00:00:00", "len": 5}')
    fhs.start()  # Starts monitor thread
    while 1:
        time.sleep(1)


if __name__ == '__main__':
    main()
