import asyncio
from typing import Optional, List, Union, Any, Dict

import _rclpy
from rclpy.node import Node, check_is_valid_msg_type, check_is_valid_srv_type
from rclpy.context import Context
from rclpy.qos import QoSProfile, qos_profile_rosout_default, qos_profile_services_default
from rclpy.parameter import Parameter
from rclpy.subscription import SubscriptionEventCallbacks

from .subscription import AsyncioSubscription
from .service import AsyncioService
from .client import AsyncioClient


AsyncioEntity = Union[AsyncioService, AsyncioClient, AsyncioSubscription]


class AsyncioNode(Node):
    def __init__(
        self,
        node_name: str,
        *,
        context: Optional[Context] = None,
        cli_args: Optional[List[str]] = None,
        namespace: Optional[str] = None,
        use_global_arguments: bool = True,
        enable_rosout: bool = True,
        rosout_qos_profile: Union[QoSProfile, int] = qos_profile_rosout_default,
        start_parameter_services: bool = True,
        parameter_overrides: Optional[List[Parameter[Any]]] = None,
        allow_undeclared_parameters: bool = False,
        automatically_declare_parameters_from_overrides: bool = False,
        enable_logger_service: bool = False
    ) -> None:
        super().__init__(
            node_name = node_name,
            context = context,
            cli_args = cli_args,
            namespace = namespace,
            use_global_arguments = use_global_arguments,
            enable_rosout = enable_rosout,
            rosout_qos_profile = rosout_qos_profile,
            start_parameter_services = start_parameter_services,
            parameter_overrides = parameter_overrides,
            allow_undeclared_parameters = allow_undeclared_parameters,
            automatically_declare_parameters_from_overrides = automatically_declare_parameters_from_overrides,
            enable_logger_service = enable_logger_service
        )

        self._tg: Optional[asyncio.TaskGroup] = None
        self._runners: Dict[AsyncioEntity, asyncio.Task] = {}
              
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, *_exc):
        await self.close()

    async def close(self):
        self._context.untrack_node(self)
        # Drop extra reference to parameter event publisher.
        # It will be destroyed with other publishers below.
        self._parameter_event_publisher = None
        
        async with asyncio.TaskGroup() as tg:
            for sub in list(self._subscriptions):
                tg.create_task(self.close_subscription(sub))
            for srv in list(self._services):
                tg.create_task(self.close_service(srv))
            for client in list(self._clients):
                tg.create_task(self.close_client(client))

        self._type_description_service.destroy()
        self.handle.destroy_when_not_in_use()

    async def run(self):
        try:
            async with asyncio.TaskGroup() as tg:
                self._tg = tg
                for sub in self._subscriptions:
                    task = tg.create_task(self._run_subscription(sub))
                    self._runners[sub] = task
                for srv in self._services:
                    task = tg.create_task(self._run_service(srv))
                    self._runners[srv] = task
                for client in self._clients:
                    task = tg.create_task(self._run_client(client))
                    self._runners[client] = task
        finally:
            self._tg = None

    # TODO: do we want a concurrent=False flag that awaits the callback?
    # TODO: do we want to utilize asyncio's eager_start on 3.12+?
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
        with client:
            async for response, sequence_number in client:
                try:
                    future = client.get_pending_request(sequence_number)
                except KeyError:
                    continue

                future.set_result(response)

    def create_subscription(
        self, msg_type, topic, callback, qos_profile, *, raw=False,
        event_callbacks=None,
    ):
        qos_profile = self._validate_qos_or_depth_parameter(qos_profile)
        check_is_valid_msg_type(msg_type)
        with self.handle:
            subscription_impl = _rclpy.Subscription(
                self.handle, msg_type, topic,
                qos_profile.get_c_qos_profile(), None)
        sub = AsyncioSubscription(
            subscription_impl, msg_type, topic, callback,
            self.default_callback_group, qos_profile, raw,
            event_callbacks or SubscriptionEventCallbacks())
        self._subscriptions.append(sub)
        if self._tg:
            task = self._tg.create_task(self._run_subscription(sub))
            self._runners[sub] = task
        return sub

    def create_service(
        self, srv_type, srv_name, callback, *,
        qos_profile=qos_profile_services_default,
    ):
        check_is_valid_srv_type(srv_type)
        with self.handle:
            service_impl = _rclpy.Service(
                self.handle, srv_type, srv_name,
                qos_profile.get_c_qos_profile())
        srv = AsyncioService(
            service_impl, srv_type, srv_name, callback,
            self.default_callback_group, qos_profile)
        self._services.append(srv)
        if self._tg:
            task = self._tg.create_task(self._run_service(srv))
            self._runners[srv] = task
        return srv

    def create_client(
        self, srv_type, srv_name, *,
        qos_profile=qos_profile_services_default,
    ):
        check_is_valid_srv_type(srv_type)
        with self.handle:
            client_impl = _rclpy.Client(
                self.handle, srv_type, srv_name,
                qos_profile.get_c_qos_profile())
        client = AsyncioClient(
            self.context, client_impl, srv_type, srv_name,
            qos_profile, self.default_callback_group)
        self._clients.append(client)
        if self._tg:
            task = self._tg.create_task(self._run_client(client))
            self._runners[client] = task
        return client

    async def close_subscription(self, sub: AsyncioSubscription):
        """Stop processing new messages and wait for existing callbacks to complete."""
        task = self._runners.pop(sub, None)
        if task is not None:
            task.cancel()
            await task
        self._subscriptions.remove(sub)

    async def close_service(self, srv: AsyncioService):
        """Stop processing new requests and wait for existing callbacks to complete."""
        task = self._runners.pop(srv, None)
        if task is not None:
            task.cancel()
            await task
        self._services.remove(srv)

    async def close_client(self, client: AsyncioClient):
        """Stop allowing new calls and cancel all pending calls."""
        task = self._runners.pop(client, None)
        if task is not None:
            task.cancel()
            await task
        self._clients.remove(client)
