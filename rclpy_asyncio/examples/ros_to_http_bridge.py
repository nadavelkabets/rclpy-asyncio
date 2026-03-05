import asyncio

import aiohttp
import rclpy
from std_srvs.srv import SetBool
from rclpy_asyncio import AsyncioNode

HTTP_TIMEOUT_SEC = 3.0
LIGHT_URL_ADDRESS = "http://light.local/set"


async def handle_http_set(
    request: SetBool.Request,
    response: SetBool.Response,
) -> SetBool.Response:
    state_param = "on" if request.data else "off"
    url = f"{LIGHT_URL_ADDRESS}?state={state_param}"
    
    try:
        async with asyncio.timeout(HTTP_TIMEOUT_SEC):
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    response.success = True
                    response.message = await resp.text()

    except asyncio.TimeoutError:
        response.success = False
        response.message = f"HTTP GET timed out after {HTTP_TIMEOUT_SEC}s"
    
    return response


async def main() -> None:
    rclpy.init()
    try:
        async with AsyncioNode("light_controller") as node:
            node.create_service(SetBool, "/light/set", handle_http_set)
            node.get_logger().info("HTTP light controller ready")
    finally:
        rclpy.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
