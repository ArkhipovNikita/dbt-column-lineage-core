from typing import List, Optional

from pglast.parser import Token


class TokenList:
    def __init__(self, tokens: List[Token]):
        self._tokens = tokens

    def real_slice(self, start: Optional[int] = None, stop: Optional[int] = None) -> "TokenList":
        start = start or 0
        end = stop or self._tokens[-1].end + 1

        return TokenList(
            list(
                filter(
                    lambda token: token.start >= start and token.end < end,
                    self._tokens,
                ),
            )
        )

    def __getitem__(self, key):
        return self._tokens[key]

    def __iter__(self):
        self._i = 0
        return self

    def __next__(self):
        if self._i < len(self._tokens):
            res = self._tokens[self._i]
            self._i += 1
            return res
        else:
            raise StopIteration

    def __len__(self):
        return len(self._tokens)

    def __delitem__(self, key):
        del self._tokens[key]
