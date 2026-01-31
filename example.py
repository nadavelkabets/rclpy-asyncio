import asyncio

async def main():
    async with AsyncioNode("listener") as node:
        node.create_subscription("/imu", Imu, on_imu)
        node.create_service("/get_pose", GetPose, handle_get_pose)
        client = node.create_client("/other_service", OtherService)
        await node.run()


async def handle_get_pose(request, response):
    response.x = 1.0
    response.y = 2.0
    return response


async def on_imu(msg):
    # Example: call a service from within a subscription callback
    response = await client.call(OtherService.Request())
    print(response)
