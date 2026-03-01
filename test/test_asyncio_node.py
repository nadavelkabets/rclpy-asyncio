import asyncio
import threading

import pytest
import rclpy
import rclpy.executors
from rclpy.node import Node as SyncNode
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
from std_msgs.msg import String
from std_srvs.srv import SetBool

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


@pytest.mark.asyncio
async def test_client_calls_service():
    """AsyncioNode client.call() sends a request to a sync service node."""
    helper = SyncNode("test_srv_helper")
    executor = rclpy.executors.SingleThreadedExecutor()
    executor.add_node(helper)

    def sync_handler(request, response):
        response.success = request.data
        response.message = "from_sync"
        return response

    helper.create_service(SetBool, "/test_client_service", sync_handler)

    spin_thread = threading.Thread(target=executor.spin, daemon=True)
    spin_thread.start()

    try:
        async with AsyncioNode("test_client_node") as node:
            client = node.create_client(SetBool, "/test_client_service")

            try:
                await asyncio.sleep(0.5)  # DDS discovery

                assert client.wait_for_service(timeout_sec=5.0)

                async with asyncio.timeout(5):
                    response = await client.call(SetBool.Request(data=True))

                assert response.success is True
                assert response.message == "from_sync"
            finally:
                await node.close()
    finally:
        executor.shutdown()
        spin_thread.join(timeout=5)
        helper.destroy_node()


@pytest.mark.asyncio
async def test_client_calls_async_service():
    """Client can call a service hosted by another AsyncioNode."""
    async def handler(request, response):
        response.success = not request.data
        response.message = "inverted"
        return response

    async with AsyncioNode("test_full_srv_node") as srv_node:
        srv_node.create_service(SetBool, "/test_full_service", handler)

        async with AsyncioNode("test_full_client_node") as client_node:
            client = client_node.create_client(SetBool, "/test_full_service")

            try:
                await asyncio.sleep(0.5)  # DDS discovery

                assert client.wait_for_service(timeout_sec=5.0)

                async with asyncio.timeout(5):
                    response = await client.call(SetBool.Request(data=True))

                assert response.success is False
                assert response.message == "inverted"
            finally:
                await client_node.close()
        await srv_node.close()
