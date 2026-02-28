import asyncio

from rclpy.subscription import Subscription


class AsyncioSubscription(Subscription):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._read_event = asyncio.Event()

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            msg_and_info = self.handle.take_message(self.msg_type, raw=False)
            if msg_and_info is not None:
                return msg_and_info[0]

            try:
                self._read_event.clear()
                await self._read_event.wait()
            except asyncio.CancelledError:
                raise StopAsyncIteration

    def _handle_new_message(self, _num_waiting: int) -> None:
        asyncio.get_running_loop().call_soon_threadsafe(self._read_event.set)

    def __enter__(self):
        self.handle.set_on_new_message_callback(self._handle_new_message)
        return self

    def __exit__(self, *_exc):
        self.handle.clear_on_new_message_callback()
        super().destroy()

    def destroy(self):
        raise NotImplementedError("Use node.close_subscription to destroy the subscription object")
