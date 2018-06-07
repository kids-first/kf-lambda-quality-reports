Daily Quality Reporting
=======================

Home of quality reports and checks that are run daily at 2AM.


Writing a Good Report
---------------------

A good report will be responsible for doing a handful of things:

- Use defined metrics and clear visualizations, when applicable
- Alerting stakeholders of interesting or erroneous data
- Save the numbers and figures in easy to parse and retrieve formats

Quantities should be defined cleary, (`Users` vs `Number of Users`), and
consistently (all rows have same dimension, no nested tables). Visualization
should follow good practices. See this ([guide](https://www3.nd.edu/~pkamat/pdf/graphs.pdf)
or this [one](https://guides.library.duke.edu/datavis/topten) for more.


A report may return attachments that will be sent to slack after the report
has been compiled. This is useful for alerting users of interesting data
or bring attention to problems.


Each report is given an `output` path in s3 where it is expected to dump its
results. A report may also return a list of local files to be uploaded directly
to slack, making them even more accessible.


Developing a New Report
-----------------------

Reports are added as new modules within the `reports` module. They are expected
to have a `handler(event, context)` function which acts similar to the standard
aws lambda handler. 
To add a new report to the daily run, add a new entry in the `reports` inside
the `invoker.py` `handler()` function with the name and module of the new
report

The `handler(event, context)` function of the report is expected to return
a tuple of `(attachments, files)`. The `attachments` is a list of
[Slack attachments](https://api.slack.com/docs/message-attachments) that will
be sent to slack after the report is run and the `files` is a dict of
`{name: path}` entries where the file in each `path` in the report's environment
will be uploaded to slack and titled with `name` as well as uploaded to s3.

Here is a basic example report function demonstrating the minimum format:
```python
def handler(event, context):
    attachments = [ { "text": "Hello World" } ]

    files = {
        'Many digits of pi': '/tmp/pi.txt'
    }

    return attachments, files
```
