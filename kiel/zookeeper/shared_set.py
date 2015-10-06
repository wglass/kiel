import json
import logging


log = logging.getLogger(__name__)


class SharedSet(object):
    """
    A simple "set" construct in Zookeeper with locking and change callbacks.

    Used by the Zookeeper-based ``GroupedConsumer`` to represent the shared set
    of topic partitions divvied up among the group.
    """
    def __init__(self, client, path, on_change):
        self.client = client

        self.path = path

        self.on_change = on_change

    @property
    def lock_path(self):
        """
        Property representing the znode path of the shared lock.
        """
        return self.path + "/lock"

    def start(self):
        """
        Creates the set's znode path and attaches the data-change callback.
        """
        self.client.ensure_path(self.path)

        @self.client.DataWatch(self.path)
        def set_changed(data, stat):
            if data is not None:
                data = self.deserialize(data)

            self.on_change(data)

    def add_items(self, new_items):
        """
        Updates the shared set's data with the given new items added.

        If all of the given items are already present, no data is updated.

        Works entirely behind a zookeeper lock to combat resource contention
        among sharers of the set.
        """
        with self.client.Lock(self.lock_path):
            existing_items = self.deserialize(self.client.get(self.path)[0])
            if not existing_items:
                existing_items = set()

            if new_items.issubset(existing_items):
                return

            existing_items.update(new_items)

            self.client.set(
                self.path, self.serialize(existing_items)
            )

    def remove_items(self, old_items):
        """
        Updates the shared set's data with the given items removed.

        If none of the given items are present, no data is updated.

        Works entirely behind a zookeeper lock to combat resource contention
        among sharers of the set.
        """
        with self.client.Lock(self.lock_path):
            existing_items = self.deserialize(self.client.get(self.path)[0])

            if old_items.isdisjoint(existing_items):
                return

            existing_items.difference_update(old_items)

            self.client.set(self.path, self.serialize(existing_items))

    def serialize(self, data):
        """
        Serializes the set data as a list in a JSON string.
        """
        return json.dumps(list(data))

    def deserialize(self, data):
        """
        Parses a given JSON string as a list, converts to a python set.
        """
        return set(json.loads(data or "[]"))
