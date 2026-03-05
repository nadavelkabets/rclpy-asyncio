import asyncio

import aiohttp
import rclpy
from std_srvs.srv import SetBool
from rclpy_asyncio import AsyncioNode

HTTP_TIMEOUT_SEC = 3.0
LIGHT_URL_ADDRESS = "http://light.local/set"


class LightControllerNode(AsyncioNode):
    def __init__(self, session: aiohttp.ClientSession) -> None:
        super().__init__("light_controller")
        self._session = session
        self.create_service(SetBool, "/light/set", self._handle_set)

    async def _handle_set(
        self,
        request: SetBool.Request,
        response: SetBool.Response,
    ) -> SetBool.Response:
        state_param = "on" if request.data else "off"
        url = f"{LIGHT_URL_ADDRESS}?state={state_param}"

        try:
            async with asyncio.timeout(HTTP_TIMEOUT_SEC):
                async with self._session.get(url) as resp:
                    resp.raise_for_status()
                    response.success = True
                    response.message = await resp.text()

        except asyncio.TimeoutError:
            response.success = False
            response.message = f"HTTP GET timed out after {HTTP_TIMEOUT_SEC}s"
        except aiohttp.ClientError as e:
            response.success = False
            response.message = f"HTTP request failed: {e}"

        return response


async def main() -> None:
    rclpy.init()
    try:
        async with aiohttp.ClientSession() as session:
            async with LightControllerNode(session) as node:
                node.get_logger().info("HTTP light controller ready")
    finally:
        rclpy.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
