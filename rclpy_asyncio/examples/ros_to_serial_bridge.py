import asyncio

import rclpy
import serial_asyncio
from std_msgs.msg import Byte
from rclpy_asyncio import AsyncioNode

SERIAL_PORT = "/dev/ttyUSB0"
BAUD_RATE = 115200


class SerialBridgeNode:
    def __init__(
        self,
        node: AsyncioNode,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self._node = node
        self._reader = reader
        self._writer = writer
        self._publisher = node.create_publisher(Byte, "/serial/rx", 10)
        node.create_subscription(
            Byte, "/serial/tx", self._handle_serial_write, 10
        )

    async def read_serial_loop(self) -> None:
        while True:
            data = await self._reader.read(1)
            if not data:
                raise ConnectionError("Serial port disconnected")
            msg = Byte()
            msg.data = data
            self._publisher.publish(msg)

    async def _handle_serial_write(self, msg: Byte) -> None:
        self._writer.write(msg.data)
        await self._writer.drain()


async def main() -> None:
    rclpy.init()
    try:
        reader, writer = await serial_asyncio.open_serial_connection(
        url=SERIAL_PORT, baudrate=BAUD_RATE
        )
        async with AsyncioNode("ros_serial_bridge") as node:
            serial_bridge = SerialBridgeNode(node, reader, writer)
            await serial_bridge.read_serial_loop()

    finally:
        writer.close()
        await writer.wait_closed()
        rclpy.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
