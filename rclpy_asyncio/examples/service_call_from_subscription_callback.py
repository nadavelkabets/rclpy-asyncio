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
        await self._client.call(SetBool.Request(data=msg.data))

async def main() -> None:
        async with AsyncioNode("demo_node") as node:
            TopicToService(node)

if __name__ == "__main__":
    rclpy.init()
    try:
        asyncio.run(main())
    finally:
        rclpy.shutdown()
