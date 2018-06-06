from botocore.vendored import requests


def handler(event, context):
    """
    Try to resolve the module from the event, then import and run
    """
    package = event.get('module', None)
    if package is None:
        return

    sub = package.split('.')[-1]

    module = __import__(package, globals(), locals(), [sub], 0)
    print('calling', module)
    module.handler(event, {})


if __name__ == '__main__':
    handler({'name': 'counts',
             'module': 'reports.counts',
             'output': 'kf-reports-us-east-1-env-quality-reports/today/counts/'}, {})
