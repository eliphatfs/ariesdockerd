import jwt
import datetime
from .config import get_config
from .error import AriesError
from typing_extensions import Literal


def run_auth(token: str):
    try:
        decoded = jwt.decode(token, get_config().jwt_key)
        if 'user' not in decoded:
            raise AriesError(4, 'user not found in token, problem with token issuer')
        if 'kind' not in decoded:
            raise AriesError(5, 'kind not found in token, problem with token issuer')
        if decoded['kind'] not in ['user', 'daemon']:
            raise AriesError(6, 'unexpected kind for auth: ' + str(decoded['kind']))
        return decoded
    except jwt.InvalidSignatureError:
        raise AriesError(3, 'invalid token')
    except jwt.ExpiredSignatureError:
        raise AriesError(2, 'token expired')


def issue(user: str, kind: Literal['user', 'daemon'] = 'user', exp: int = 300):
    return jwt.encode(dict(
        exp=datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=exp),
        user=user,
        kind=kind,
        v=1
    ), get_config().jwt_key)
