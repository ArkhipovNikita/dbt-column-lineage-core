from typing import List

from pglast.parser import Token


class TokenList:
    def __init__(self, tokens: List[Token]):
        self._tokens = tokens

    def __getitem__(self, key):
        if not isinstance(key, slice):
            raise TypeError

        start = key.start or 0
        end = key.stop or self._tokens[-1].end + 1

        return list(
            filter(
                lambda token: token.start >= start and token.end < end,
                self._tokens,
            ),
        )
