# AWS Firehose Sender

This Python 2.7 class can be used to implement a simple and safe AWS Firehose Sender.

## Overview

FirehoseSender handles data to send by writing strings to a file; when this file reachs a predefined number of lines FirehoseSender will move it to a specific dir and then send it to the stream in the next thread iteration. This method prevents data loss caused by connection problems.

So we have three dirs:

* tmp\_out: files that are not completed
* ready\_out: files ready to be send
* sent\_out: (only if enabled) files already sent

This class is mainly designed to be used with JSON and so we have some methods that helps for this purpose, like:

* prepare\_json: that creates a simple JSON string passing a dict 
* check\_integrity: that checks integrity of files with JSON strings (_escaped curly brackets are not handled_) that could have been corrupted by an unexpected closure 

That doesn't mean that this class can't be used without JSON. To do this you must set to false a parameter on the constructor called "json", otherwise check\_integrity (always called by the main thread) will remove any data that is not JSON.

## Use

In the main() function you can see a little example of use that writes 10 JSON strings and send them to a stream set in 'firehose.conf':

```python
fhs = FirehoseSender.from_config('firehose.conf')  # Takes client configuration from file
for i in range(10):  # Write a file that is ready to be sent
        fhs.write_element('{"id": 1, "some_key": "some_value", "acqua": "water", "datetime":'
                          ' "2018-06-21 00:00:00", "len": 5}')
fhs.start()  # Starts monitor thread
while 1:
        time.sleep(1)
```

FirehoseSender can be instantiated in two ways:

* using the constructor: 
```python
def __init__(self, access_key, secret_key, stream_name, max_rows=10, remove_sent=False, json=True)
```
* using from\_config(), that uses a ini file like in the previous example

After that you need to start "monitor thread" with start(), and then this thread will monitor the three dirs.
With write\_element() you can write a string to send, that could have been preprocessed with prepare\_json().

## Dependencies

You will need *boto3* and *ConfigParser* in order to use this class.

```sh
pip install boto3
pip install ConfigParser
```

## Python 3

This class can be easily ported in Python 3, you'll just need to change all print statements, ConfigParser to configparser and SafeConfigParser to ConfigParser.


