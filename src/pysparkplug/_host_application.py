from __future__ import annotations

import logging
from typing import Callable, Iterable, Optional

from pysparkplug._client import Client
from pysparkplug._constants import (
    DEFAULT_CLIENT_BIND_ADDRESS,
    DEFAULT_CLIENT_BLOCKING,
    DEFAULT_CLIENT_KEEPALIVE,
    DEFAULT_CLIENT_PORT,
)
from pysparkplug._datatype import DataType
from pysparkplug._enums import MessageType, QoS
from pysparkplug._message import Message
from pysparkplug._metric import Metric
from pysparkplug._payload import DCmd,NCmd,State
from pysparkplug._time import get_current_timestamp
from pysparkplug._topic import Topic
from pysparkplug._types import Self
from pysparkplug._constants import SINGLE_LEVEL_WILDCARD


logger = logging.getLogger(__name__)
def _default_message_callback(_: "HostApplication", message: Message) -> None:
        """Default callback if the user does not supply their own.

        For a Host Application, this might be a simple logger. In a real system,
        you would parse NBIRTH, DBIRTH, NDATA, DDATA, etc., and handle them
        accordingly.
        """
        logger.info(f"[Host] Received message: {message}")


class HostApplication:
    """Class representing a Sparkplug Host Application.

    In Sparkplug, a Host Application typically supervises many Edge Nodes. It:
        - Subscribes to NBIRTH / DBIRTH / NDATA / DDATA / NDEATH / DDEATH, etc.
        - Publishes NCMD or DCMD messages back to Edge Nodes.
        - Publishes its own STATE message to indicate online/offline.

    Args:
        host_id:
            A unique ID for this Host Application (used in `spBv1.0/STATE/<host_id>`).
        client:
            An optional low-level MQTT `Client`. If not provided, a default `Client()` is used.
        message_callback:
            A callback invoked whenever *any* Sparkplug message is received. In a Host,
            you typically handle births, data updates, etc., in this callback.
    """

    host_id: str
    _client: Client

    _connected: bool = False
    _message_callback: Callable[["HostApplication", Message], None]

    def __init__(
        self,
        host_id: str,
        client: Optional[Client] = None,
        message_callback: Callable[[Self, Message], None] = _default_message_callback,
        retain_birth_certificates: bool = False,
    ):
        self.host_id = host_id
        self._client = client if client is not None else Client()
        self._message_callback = message_callback

        # Subscribe to all "Host State" messages about ourselves if desired.
        # (Some users do this for debugging or to watch their own birth/death,
        # but it is optional.)
        self.subscribe(
            topic=Topic(message_type=MessageType.STATE, group_id=SINGLE_LEVEL_WILDCARD, sparkplug_host_id=self.host_id),
            qos=QoS.AT_LEAST_ONCE,
            callback=self._message_callback,
        )

    def _setup_will(self) -> None:
        """Set the Host's 'last will' STATE message.

        If this HostApplication disconnects unexpectedly, the broker
        will automatically publish this will indicating the Host is offline.
        """
        will_topic = Topic(message_type=MessageType.STATE, sparkplug_host_id=self.host_id)
        will_payload = State(timestamp=get_current_timestamp(), online=False)
        will_message = Message(topic=will_topic, payload=will_payload, qos=QoS.AT_MOST_ONCE, retain=True)
        self._client.set_will(will_message)

    def connect(
        self,
        host: str,
        *,
        port: int = DEFAULT_CLIENT_PORT,
        keepalive: int = DEFAULT_CLIENT_KEEPALIVE,
        bind_address: str = DEFAULT_CLIENT_BIND_ADDRESS,
        blocking: bool = DEFAULT_CLIENT_BLOCKING,
        on_connect: Optional[Callable[["HostApplication"], None]] = None,
    ) -> None:
        """Connect this Host Application to the broker.

        Publishes an online STATE message upon successful connect.

        Args:
            host:
                The hostname or IP address of the remote broker.
            port:
                The MQTT broker port.
            keepalive:
                Maximum period in seconds allowed between communications.
            bind_address:
                Local address to bind to (if multiple interfaces).
            blocking:
                True to block until connection completes, otherwise connect asynchronously.
        """
        # Set the will for the next connection.
        self._setup_will()

        def on_connect_callback(client: Client) -> None:
            self._connected = True
            # Publish STATE that the Host is online
            online_topic = Topic(message_type=MessageType.STATE, sparkplug_host_id=self.host_id) #could make a conflict with the subscription 
            online_payload = State(timestamp=get_current_timestamp(), online=True)
            online_message = Message(topic=online_topic, payload=online_payload, qos=QoS.AT_LEAST_ONCE, retain=True)
            client.publish(online_message)
            
            if on_connect is not None:
                on_connect(self)

            # Reset the will for next time.
            self._setup_will()

        self._client.connect(
            host,
            port=port,
            keepalive=keepalive,
            bind_address=bind_address,
            blocking=blocking,
            callback=on_connect_callback,
        )

    def disconnect(self) -> None:
        """Gracefully disconnect from the broker, publishing a final offline STATE."""
        if self._connected:
            offline_topic = Topic(message_type=MessageType.STATE, sparkplug_host_id=self.host_id)
            offline_payload = State(timestamp=get_current_timestamp(), online=False)
            offline_message = Message(topic=offline_topic, payload=offline_payload, qos=QoS.AT_MOST_ONCE, retain=True)
            self._client.publish(offline_message)

        self._client.disconnect()
        self._connected = False
    #add some flexibility to the subscription
    def subscribe(
        self,
        topic: Topic,
        qos: QoS,
        callback: Optional[Callable[[Self, Message], None]] = None,
    ) -> None:
        """Subscribe to a Sparkplug topic with a callback.

        In a Host Application, you typically subscribe to:
         - spBv1.0/<group_id>/#/NBIRTH
         - spBv1.0/<group_id>/#/DBIRTH
         - spBv1.0/<group_id>/#/NDATA
         - spBv1.0/<group_id>/#/DDATA
         - spBv1.0/<group_id>/#/NDEATH
         - spBv1.0/<group_id>/#/DDEATH
        Or simply subscribe to `spBv1.0/<group_id>/#` to capture everything.

        Args:
            topic:   Topic object specifying e.g. NBIRTH, NDATA, etc.
            qos:     Sparkplug QoS. Typically AT_LEAST_ONCE or AT_MOST_ONCE.
            callback:
                The callback to invoke when messages arrive on this subscription.
                Defaults to self._message_callback if None is provided.
        """

        def cb(_client: Client, message: Message) -> None:
            # In an actual system, you might parse message.payload for NBIRTH,
            # NDATA, etc. For this example, just pass to caller’s callback.
            (callback or self._message_callback)(self, message)

        self._client.subscribe(topic, qos, cb)

    def unsubscribe(self, topic: Topic) -> None:
        """Unsubscribe from a Sparkplug topic."""
        self._client.unsubscribe(topic)

    def send_node_command(
        self,
        edge_node_id: str,
        metrics: list,
        *,
        group_id: str = None,
        qos: QoS = QoS.AT_LEAST_ONCE,
        retain: bool = False,
    ) -> None:
        """Send an NCMD (Node Command) to a given Edge Node.

        Args:
            edge_node_id:
                The target node’s unique ID (as in spBv1.0/<group_id>/<edge_node_id>).
            metrics:
                A list of Metric objects you want to send in the NCMD.
            qos:
                Publish QoS level.
            retain:
                Whether to retain the message on the broker.
        """
        topic = Topic(
            message_type=MessageType.NCMD,
            group_id=group_id,
            edge_node_id=edge_node_id,
        )
        payload = NCmd(timestamp=get_current_timestamp(), metrics=metrics)
        msg = Message(topic=topic, payload=payload, qos=qos, retain=retain)
        self._client.publish(msg, include_dtypes=True)

    def send_device_command(
        self,
        edge_node_id: str,
        device_id: str,
        metrics: list,
        *,
        group_id: str = None,
        qos: QoS = QoS.AT_MOST_ONCE,
        retain: bool = False,
    ) -> None:
        """Send a DCMD (Device Command) to a given Device under an Edge Node.

        Args:
            edge_node_id:
                The parent node’s unique ID.
            device_id:
                The target device’s unique ID.
            metrics:
                A list of Metric objects for the DCMD.
            qos:
                Publish QoS level.
            retain:
                Whether to retain the message on the broker.
        """
        topic = Topic(
            message_type=MessageType.DCMD,
            group_id=group_id,
            edge_node_id=edge_node_id,
            device_id=device_id,
        )
        payload = DCmd(timestamp=get_current_timestamp(), metrics=metrics)
        msg = Message(topic=topic, payload=payload, qos=qos, retain=retain)
        self._client.publish(msg, include_dtypes=True)

    @property
    def connected(self) -> bool:
        """Returns whether this HostApplication is connected to the broker."""
        return self._connected


def get_topic_hierarchy_from_spb_parris_encoding(message: Message, metric: Metric) -> list[str]:
    if not message.topic.namespace == "spBv1.0":
        raise ValueError(f"Unsupported namespace: {message.topic.namespace}")
    
    topic_hierarchy = message.topic.group_id.split(":")
    
    if message.topic.edge_node_id is not None and message.topic.sparkplug_host_id is not None:
        raise ValueError("Edge Node and Host ID are mutually exclusive.")
    elif message.topic.edge_node_id is not None:
        topic_hierarchy.append(message.topic.edge_node_id)
        if message.topic.device_id is not None:
            topic_hierarchy.append(message.topic.device_id)
    elif message.topic.sparkplug_host_id is not None:
        topic_hierarchy.append(message.topic.sparkplug_host_id)
    
    topic_hierarchy.extend(metric.name.split(":"))
    
    return topic_hierarchy
