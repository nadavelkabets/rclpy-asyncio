import asyncio

import rclpy
from std_msgs.msg import Bool
from std_srvs.srv import SetBool

from rclpy_asyncio import AsyncioNode


class TopicToService:
    def __init__(self, node: AsyncioNode):
        self._node = node
        self._client = node.create_client(SetBool, "/set_bool")

        node.create_subscription(Bool, "/bool", self._on_message, 10)
    
    async def _on_message(self, msg: Bool) -> None:
        await self._node.sleep(2)
        response = await self._client.call(SetBool.Request(data=msg.data))
        self._node.get_logger().info(
            f"Service response: success={response.success}, message='{response.message}'")


async def main() -> None:
    rclpy.init()
    try:
        async with AsyncioNode("demo_node") as node:
            TopicToService(node)
    finally:
        rclpy.shutdown()


if __name__ == "__main__":
        asyncio.run(main())
