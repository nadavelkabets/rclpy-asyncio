import asyncio

import pytest
from rclpy.node import Node as SyncNode
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
from std_msgs.msg import String

from rclpy_asyncio.node import AsyncioNode

TEST_QOS = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    history=HistoryPolicy.KEEP_LAST,
    depth=10,
)


@pytest.mark.asyncio
async def test_lifecycle():
    """Node creates and destroys cleanly via async context manager."""
    async with AsyncioNode("test_lifecycle_node") as node:
        await node.close()


@pytest.mark.asyncio
async def test_subscription_receives_message():
    """Subscription callback fires when a message is published."""
    received = asyncio.Event()
    received_data = []

    async def callback(msg):
        received_data.append(msg.data)
        received.set()

    async with AsyncioNode("test_sub_node") as node:
        node.create_subscription(String, "/test_sub_topic", callback, TEST_QOS)

        helper = SyncNode("test_pub_helper")
        pub = helper.create_publisher(String, "/test_sub_topic", TEST_QOS)

        try:
            await asyncio.sleep(0.5)  # DDS discovery
            pub.publish(String(data="hello"))

            async with asyncio.timeout(5):
                await received.wait()

            assert received_data == ["hello"]
        finally:
            helper.destroy_node()
            await node.close()
