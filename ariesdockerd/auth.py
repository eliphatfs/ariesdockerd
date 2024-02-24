import jwt
import datetime
from .config import get_config


class AuthError(Exception):
    pass


def run_auth(token: str):
    try:
        decoded = jwt.decode(token, get_config().jwt_key)
        if 'user' not in decoded:
            raise AuthError(1, 'user not found in token, problem with token issuer')
        return decoded
    except jwt.InvalidSignatureError:
        raise AuthError(2, 'invalid token')
    except jwt.ExpiredSignatureError:
        raise AuthError(3, 'token expired')


def issue(user: str, exp: int = 300):
    return jwt.encode(dict(
        exp=datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=exp),
        user=user
    ), get_config().jwt_key)
