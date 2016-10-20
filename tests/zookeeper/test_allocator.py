import unittest

from mock import patch

from kiel.zookeeper import allocator


@patch.object(allocator, "SharedSet")
@patch.object(allocator, "Party")
class PartitionAllocatorTests(unittest.TestCase):

    def setUp(self):
        super(PartitionAllocatorTests, self).setUp()

        kazoo_patcher = patch.object(allocator, "client")
        mock_client = kazoo_patcher.start()
        self.addCleanup(kazoo_patcher.stop)

        self.KazooClient = mock_client.KazooClient

    def test_defaults(self, Party, SharedSet):

        def alloc(members, partitions):
            pass

        a = allocator.PartitionAllocator(
            ["zk01", "zk02", "zk03"], "worker-group", "worker01:654321",
            allocator_fn=alloc
        )

        self.assertEqual(a.zk_hosts, ["zk01", "zk02", "zk03"])
        self.assertEqual(a.group_name, "worker-group")
        self.assertEqual(a.consumer_name, "worker01:654321")

        self.assertEqual(a.allocator_fn, alloc)
        self.assertEqual(a.on_rebalance, None)

        self.assertEqual(a.conn, self.KazooClient.return_value)
        self.assertEqual(a.party, Party.return_value)
        self.assertEqual(a.shared_set, SharedSet.return_value)

        self.assertEqual(a.members, set())
        self.assertEqual(a.partitions, set())
        self.assertEqual(a.mapping, {})
