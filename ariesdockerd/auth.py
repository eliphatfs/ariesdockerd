import jwt
import datetime
import argparse
from .config import get_config
from .error import AriesError
from typing_extensions import Literal


def run_auth(token: str):
    try:
        decoded = jwt.decode(token, get_config().jwt_key, algorithms=["HS256"], leeway=2592000*5)
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


def issue_token_main():
    argp = argparse.ArgumentParser()
    argp.add_argument("user", type=str)
    argp.add_argument("-e", "--exp", type=int, default=86400 * 60)
    args = argp.parse_args()
    print(issue(args.user, 'user', args.exp))
