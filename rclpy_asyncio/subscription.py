from rclpy.subscription import Subscription


class AsyncioSubscription(Subscription):
    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._destroyed:
            raise RuntimeError("Unable to get the next message: subscription destroyed")
        if self._closing:
            raise StopAsyncIteration

        return await self._receive_message()

    def close(self):
        """Signal the subscription to stop iteration gracefully."""
        self._closing = True
    
    def __enter__(self):
        self._handler.set_on_new_message_callback()
        return self

    def __exit__(self, *_exc):
        self._handler.clear_on_new_message_callback()