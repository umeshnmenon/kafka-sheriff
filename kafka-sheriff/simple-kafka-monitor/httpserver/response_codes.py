"""
HTTP Success and Error Code
--------------
This module contains the Application Error Code. Each class relates to a single Application Error Code. Each class is sub class of
the :class:`~SimpleHTTPError`
SimpleHTTPError
    * 501 - SimpleHTTPErrorControllerNotExists
    * .....

Example Usages:
# minimal call
print(SimpleHTTPErrorControllerNotExists())

# with custom message
print(SimpleHTTPErrorControllerNotExists(description = 'Method does not exist'))

# with a custom json formatter
def json_formatter(code, description):
    return {'Error': {'code': code,
                      'description': description
                      }}

err = SimpleHTTPErrorControllerNotExists(description = 'The specified method does not exist', json_formatter = json_formatter)
print(err())
"""
import json
#@implementer(IErrorCode) # TODO: We can also have an interface defined
class SimpleHTTPStatus:
    """Base class for all the HTTP Status which will be passed to the requester"""
    ## Set the code, description, explanation (detail) in the sub classes
    # default set the code and description
    code = 200
    phrase = 'OK'
    description = 'Request fulfilled, document follows'
    details = ''

    def __init__(self, phrase=None, description=None, details=None, json_formatter=None):
        # Only phrase, description and explanation can be customized not code
        if phrase is not None:
            self.phrase = phrase
        if description is not None:
            self.description = description
        if details is not None:
            self.details = details
        # Use a different json_formatter if you wish to return a custom json other than the default one
        if json_formatter is not None:
            self._json_formatter = json_formatter

    def __str__(self):
        # detail =  '%s %s %s' % (self.code, self.description, self.details)
        # return str(detail)
        return (self.prepare())

    def _json_formatter(self, code, description):
        return {'code': code,
                'description': description
                }

    def prepare(self):
        resp_json = self._json_formatter(self.code, self.description)
        return json.dumps(resp_json)

    def __call__(self, *args, **kwargs):
        return self.prepare()


class SimpleHTTPError(SimpleHTTPStatus):
    """Base class for all the Application Error which will be passed to the requester"""
    ## Set the code, description, explanation (detail) in the sub classes
    # default set the code and description
    code = 500
    description = 'Unknown Error'

class SimpleHTTPErrorZeroContentLength(SimpleHTTPError):
    code = 501
    description = 'No request data passed'

class SimpleHTTPErrorInvalidRequest(SimpleHTTPError):
    code = 502
    description = 'Invalid Request'

class SimpleHTTPErrorControllerNotExists(SimpleHTTPError):
    code = 503
    description = 'No action exists with the given name'

class SimpleHTTPErrorInternalError(SimpleHTTPError):
    code = 500
    description = 'Internal Error'

#### SUCCESS CODES
class SimpleHTTPStatusOK(SimpleHTTPStatus):
    pass