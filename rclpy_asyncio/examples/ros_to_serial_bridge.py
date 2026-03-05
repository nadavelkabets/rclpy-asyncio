import asyncio

import rclpy
import serial_asyncio
from std_msgs.msg import Byte
from rclpy_asyncio import AsyncioNode

SERIAL_PORT = "/dev/ttyUSB0"
BAUD_RATE = 115200


class SerialBridgeNode(AsyncioNode):
    def __init__(self):
        super().__init__("ros_serial_bridge")
        self._writer = None
        self._reader = None
        self._publisher = None

    async def start_bridge(self, port: str, baud: int) -> None:
        self._reader, self._writer = await serial_asyncio.open_serial_connection(
            url=port, baudrate=baud
        )
        self._publisher = self.create_publisher(Byte, "/serial/rx", 10)
        self.create_subscription(
            Byte, "/serial/tx", self.handle_serial_write, 10
        )
        
    async def read_serial_loop(self) -> None:
        while True:
            data = await self._reader.read(1)
            msg = Byte()
            msg.data = data
            self._publisher.publish(msg)

    async def handle_serial_write(self, msg: Byte) -> None:
        self._writer.write(msg.data)
        await self._writer.drain()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()

        return await super().__aexit__(exc_type, exc_val, exc_tb)


async def main() -> None:
    rclpy.init()
    try:
        async with SerialBridgeNode() as node:
            await node.start_bridge(SERIAL_PORT, BAUD_RATE)
            await node.read_serial_loop()

    finally:
        rclpy.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
