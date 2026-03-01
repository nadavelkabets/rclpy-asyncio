import asyncio
from functools import partial

async def main():
    async with AsyncioNode("listener") as node:
        client = node.create_client("/other_service", OtherService)
        node.create_subscription("/imu", Imu, partial(on_imu, client))
        node.create_service("/get_pose", GetPose, handle_get_pose)


async def handle_get_pose(_request, response):
    response.x = 1.0
    response.y = 2.0
    return response


async def on_imu(client, msg):
    # Example: call a service from within a subscription callback
    async with asyncio.Timeout(5):
        response = await client.call(OtherService.Request())
    print(response)
