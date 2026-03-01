import asyncio
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Type, Union

from rclpy.client import Client
from rclpy.context import Context
from rclpy.executors import await_or_execute
from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.qos import QoSProfile, qos_profile_rosout_default, qos_profile_services_default
from rclpy.service import Service
from rclpy.subscription import Subscription, SubscriptionCallbackUnion
from rclpy.type_support import MsgT, Srv, SrvRequestT, SrvResponseT


Entity = Union[Subscription, Service, Client]


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
        self._tg: Optional[asyncio.TaskGroup] = None
        self._runners: Dict[Entity, asyncio.Task[None]] = {}

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

    async def __aenter__(self) -> 'AsyncioNode':
        tg = asyncio.TaskGroup()
        self._tg = await tg.__aenter__()
        for sub in self._subscriptions:
            task = self._tg.create_task(self._run_subscription(sub))
            self._runners[sub] = task
        for srv in self._services:
            task = self._tg.create_task(self._run_service(srv))
            self._runners[srv] = task
        for client in self._clients:
            task = self._tg.create_task(self._run_client(client))
            self._runners[client] = task
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        tg = self._tg
        self._tg = None
        await tg.__aexit__(exc_type, exc_val, exc_tb)
        self._context.untrack_node(self)
        self._parameter_event_publisher = None
        self._type_description_service.destroy()
        self.handle.destroy_when_not_in_use()

    async def close(self) -> None:
        async with asyncio.TaskGroup() as tg:
            for sub in list(self._subscriptions):
                tg.create_task(self.close_subscription(sub))
            for srv in list(self._services):
                tg.create_task(self.close_service(srv))
            for client in list(self._clients):
                tg.create_task(self.close_client(client))

    # TODO: do we want a concurrent=False flag that awaits the callback?
    # TODO: do we want to utilize asyncio's eager_start on 3.12+?
    async def _run_subscription(self, subscription: Subscription[MsgT]) -> None:
        """Node owns the DDS bridge and read loop for subscriptions."""
        loop = asyncio.get_running_loop()
        read_event = asyncio.Event()

        def _on_new_message(_num_waiting):
            loop.call_soon_threadsafe(read_event.set)

        subscription.handle.set_on_new_message_callback(_on_new_message)
        try:
            async with asyncio.TaskGroup() as tg:
                while True:
                    # TODO: share code with executors.py _take_subscription
                    msg_and_info = subscription.handle.take_message(
                        subscription.msg_type, subscription.raw)
                    if msg_and_info is not None:
                        if subscription._callback_type is Subscription.CallbackType.MessageOnly:
                            msg_tuple = (msg_and_info[0],)
                        else:
                            msg_tuple = msg_and_info
                        tg.create_task(await_or_execute(
                            subscription.callback, *msg_tuple))
                    else:
                        try:
                            read_event.clear()
                            await read_event.wait()
                        except asyncio.CancelledError:
                            break
        finally:
            subscription.handle.clear_on_new_message_callback()
            subscription.destroy()

    async def _run_service(self, service: Service[SrvRequestT, SrvResponseT]) -> None:
        """Node owns the DDS bridge and read loop for services."""
        loop = asyncio.get_running_loop()
        read_event = asyncio.Event()

        def _on_new_request(_num_waiting):
            loop.call_soon_threadsafe(read_event.set)

        service.handle.set_on_new_request_callback(_on_new_request)
        try:
            async with asyncio.TaskGroup() as tg:
                while True:
                    # TODO: share code with executors.py _take_service
                    request_and_header = service.handle.service_take_request(
                        service.srv_type.Request)
                    if request_and_header != (None, None):
                        tg.create_task(self._handle_service_request(
                            service, request_and_header[0], request_and_header[1]))
                    else:
                        try:
                            read_event.clear()
                            await read_event.wait()
                        except asyncio.CancelledError:
                            break
        finally:
            service.handle.clear_on_new_request_callback()
            service.destroy()

    async def _handle_service_request(
        self,
        service: Service[SrvRequestT, SrvResponseT],
        request: SrvRequestT,
        header: Any,
    ) -> None:
        response = await await_or_execute(
            service.callback, request, service.srv_type.Response())
        service.send_response(response, header)

    async def _run_client(self, client: Client[SrvRequestT, SrvResponseT]) -> None:
        """Node owns the DDS bridge and response routing for clients."""
        loop = asyncio.get_running_loop()
        read_event = asyncio.Event()

        def _on_new_response(_num_waiting):
            loop.call_soon_threadsafe(read_event.set)

        client.handle.set_on_new_response_callback(_on_new_response)
        try:
            while True:
                # TODO: share code with executors.py _take_client
                header_and_response = client.handle.take_response(
                    client.srv_type.Response)
                if header_and_response != (None, None):
                    header, response = header_and_response
                    future = client._pending_requests.get(
                        header.request_id.sequence_number)
                    if future is not None:
                        future.set_result(response)
                else:
                    try:
                        read_event.clear()
                        await read_event.wait()
                    except asyncio.CancelledError:
                        break
        finally:
            client.handle.clear_on_new_response_callback()
            for future in client._pending_requests.values():
                future.cancel()
            client._pending_requests.clear()
            client.destroy()

    def create_subscription(
        self,
        msg_type: Type[MsgT],
        topic: str,
        callback: SubscriptionCallbackUnion[MsgT],
        qos_profile: Union[QoSProfile, int],
        **kwargs: Any,
    ) -> Subscription[MsgT]:
        sub = super().create_subscription(msg_type, topic, callback, qos_profile, **kwargs)
        if self._tg:
            task = self._tg.create_task(self._run_subscription(sub))
            self._runners[sub] = task
        return sub

    def create_service(
        self,
        srv_type: Type[Srv],
        srv_name: str,
        callback: Callable[[SrvRequestT, SrvResponseT], SrvResponseT],
        **kwargs: Any,
    ) -> Service[SrvRequestT, SrvResponseT]:
        srv = super().create_service(srv_type, srv_name, callback, **kwargs)
        if self._tg:
            task = self._tg.create_task(self._run_service(srv))
            self._runners[srv] = task
        return srv

    def create_client(
        self,
        srv_type: Type[Srv],
        srv_name: str,
        **kwargs: Any,
    ) -> Client[SrvRequestT, SrvResponseT]:
        client = super().create_client(srv_type, srv_name, **kwargs)

        # TODO: replace the monkeypatch with AsyncioClient class
        async def call(request: SrvRequestT) -> SrvResponseT:
            future: asyncio.Future[SrvResponseT] = asyncio.get_running_loop().create_future()
            sequence_number = client.handle.send_request(request)
            client._pending_requests[sequence_number] = future
            try:
                return await future
            finally:
                client._pending_requests.pop(sequence_number, None)

        client.call = call

        if self._tg:
            task = self._tg.create_task(self._run_client(client))
            self._runners[client] = task
        return client

    async def close_subscription(self, sub: Subscription[MsgT]) -> None:
        """Stop processing new messages and wait for existing callbacks to complete."""
        task = self._runners.pop(sub, None)
        if task is not None:
            task.cancel()
            await task
        self._subscriptions.remove(sub)

    async def close_service(self, srv: Service[SrvRequestT, SrvResponseT]) -> None:
        """Stop processing new requests and wait for existing callbacks to complete."""
        task = self._runners.pop(srv, None)
        if task is not None:
            task.cancel()
            await task
        self._services.remove(srv)

    async def close_client(self, client: Client[SrvRequestT, SrvResponseT]) -> None:
        """Stop allowing new calls and cancel all pending calls."""
        task = self._runners.pop(client, None)
        if task is not None:
            task.cancel()
            await task
        self._clients.remove(client)
