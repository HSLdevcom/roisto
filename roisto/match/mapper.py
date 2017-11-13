# -*- coding: utf-8 -*-
"""Key-value store with updating."""


class Mapper:
    """Key-value store with updating.

    Mapper remembers if something was asked for but was not found. If it still
    cannot be found after updating, it will be ignored until the next update.
    """

    def __init__(self, update, parse):
        self._update = update
        self._parse = parse

        # Resulting data after updating and parsing.
        self._mapping = {}
        # Ignore keys that could not be resolved even after updating the
        # mapping.
        self._ignored = set()
        # Keys that are ignored but the user is still interested in. Used as
        # the basis for self._ignored after the next mapping update as
        # self._ignored should not grow indefinitely.
        self._attempted = set()
        # New keys that we are not able to resolve since last update. There is
        # no reason to update the mapping until this is not empty.
        self._missing = set()

    def get(self, key):
        """Return mapped information if found.

        Otherwise return None.
        """
        mapped = self._mapping.get(key, None)
        if mapped is None:
            if key in self._ignored:
                self._attempted.add(key)
            else:
                self._missing.add(key)
        return mapped

    async def update(self):
        """Update the mapping.

        Return True if an update was done, otherwise return False.
        """
        if self._missing:
            result = await self._update()
            self._mapping = self._parse(result)
            not_found_last_round = self._attempted.union(self._missing)
            self._ignored = not_found_last_round.difference(
                self._mapping.keys())
            self._attempted = set()
            self._missing = set()
            return True
        return False
