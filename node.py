import asyncio

from rclpy.node import Node

from .subscription import AsyncioSubscription
from .service import AsyncioService
from .client import AsyncioClient


class AsyncioNode(Node):
    async def run(self):
        async with asyncio.TaskGroup() as tg:
            self._tg = tg
            for sub in self._subscriptions:
                task = tg.create_task(self._run_subscription(sub))
                self._subscription_tasks[sub] = task
            for srv in self._services:
                task = tg.create_task(self._run_service(srv))
                self._service_tasks[srv] = task
            for client in self._clients:
                task = tg.create_task(self._run_client(client))
                self._client_tasks[client] = task
        self._tg = None

    # TODO: do we want a concurrent=False flag that awaits the callback?
    async def _run_subscription(self, subscription: AsyncioSubscription):
        """Node implements the read loop."""
        async with asyncio.TaskGroup() as tg:
            with subscription:
                async for msg in subscription:
                    tg.create_task(subscription.callback(msg))

    async def _run_service(self, service: AsyncioService):
        """Node implements the service loop with concurrent request handling."""
        async with asyncio.TaskGroup() as tg:
            with service:
                async for request, header in service:
                    tg.create_task(self._handle_service_request(service, request, header))

    async def _handle_service_request(self, service, request, header):
        response = await service.callback(request, service.srv_type.Response())
        service.send_response(response, header)

    async def _run_client(self, client: AsyncioClient):
        """Node implements the client response loop."""
        try:
            with client:
                async for response, sequence_number in client:
                    future = client._pending_requests.pop(sequence_number, None)
                    if future:
                        future.set_result(response)
        finally:
            # Cancel all pending futures if loop exits unexpectedly
            for future in client._pending_requests.values():
                future.cancel()
            client._pending_requests.clear()

    def create_subscription(self, *args):
        sub = AsyncioSubscription(self)
        self._subscriptions.add(sub)
        if self._tg:
            self._tg.create_task(self._run_subscription(sub))

        return sub

    def create_service(self, service_name, service_type, callback):
        srv = AsyncioService(self, service_name, service_type, callback)
        self._services.add(srv)
        if self._tg:
            self._tg.create_task(self._run_service(srv))
            
        return srv

    def create_client(self, service_name, service_type):
        client = AsyncioClient(self, service_name, service_type)
        self._clients.add(client)
        if self._tg:
            self._tg.create_task(self._run_client(client))

        return client

    async def close_subscription(self, sub):
        """
        Stop processing new messages and wait for existing callbacks to complete.
        """
        task = self._subscription_tasks.pop(sub)
        sub.close()
        await task

        sub.destroy()
        self._subscriptions.remove(sub)

    async def close_service(self, srv):
        """
        Stop processing new requests and wait for existing callbacks to complete.
        """
        task = self._service_tasks.pop(srv)
        srv.close()
        await task

        srv.destroy()
        self._services.remove(srv)

    async def close_client(self, client):
        """
        Stop allowing new calls and cancel all pending calls.
        """
        task = self._client_tasks.pop(client)
        client.close()
        await task

        client.destroy()
        self._clients.remove(client)