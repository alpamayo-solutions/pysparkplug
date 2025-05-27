from __future__ import annotations
 
import logging
from typing import Callable, Optional, Dict, Tuple, List
 
from pysparkplug._client import Client
from pysparkplug._constants import (
    DEFAULT_CLIENT_BIND_ADDRESS,
    DEFAULT_CLIENT_BLOCKING,
    DEFAULT_CLIENT_KEEPALIVE,
    DEFAULT_CLIENT_PORT,
)
from pysparkplug._enums import MessageType, QoS
from pysparkplug._message import Message
from pysparkplug._metric import Metric
from pysparkplug._payload import NBirth, NData, NDeath
from pysparkplug._time import get_current_timestamp
from pysparkplug._topic import Topic
from pysparkplug._types import Self
 
logger = logging.getLogger(__name__)
 
def _default_message_callback(_: "DataOpsNode", message: Message) -> None:
    logger.info(f"[DataOpsNode] Received message: {message}")
 
class DataOpsNode:
    """DataOpsNode capable of arbitrary publishing/subscribing with birth/rebirth capability."""
 
    node_id: str
    _client: Client
    _connected: bool = False
    _message_callback: Callable[[Self, Message], None]
 
    # Internal state to track published metrics for each topic
    _published_metrics: Dict[Tuple[str, str], Dict[str, Metric]]
    _retain_birth_certificates: bool = False
 
    def __init__(
        self,
        node_id: str,
        client: Optional[Client] = None,
        message_callback: Callable[[Self, Message], None] = _default_message_callback,
        retain_birth_certificates: bool = False,
    ):
        self.node_id = node_id
        self._client = client if client else Client()
        self._message_callback = message_callback
        self._published_metrics = {}
        self._retain_birth_certificates = retain_birth_certificates
 
    def connect(
        self,
        host: str,
        *,
        port: int = DEFAULT_CLIENT_PORT,
        keepalive: int = DEFAULT_CLIENT_KEEPALIVE,
        bind_address: str = DEFAULT_CLIENT_BIND_ADDRESS,
        blocking: bool = DEFAULT_CLIENT_BLOCKING,
    ) -> None:
        """Connect DataOpsNode to the broker and publish NBIRTH for known topics."""
 
        def on_connect_callback(client: Client) -> None:
            self._connected = True
            logger.info(f"[DataOpsNode:{self.node_id}] Connected to broker.")
 
            # Publish NBIRTH messages for all known topics
            for (group_id, edge_node_id), metrics in self._published_metrics.items():
                self._publish_nbirth(group_id, edge_node_id, metrics.values())
 
        self._client.connect(
            host,
            port=port,
            keepalive=keepalive,
            bind_address=bind_address,
            blocking=blocking,
            callback=on_connect_callback,
        )
 
    def disconnect(self) -> None:
        """Publish NDEATH for all known topics and disconnect cleanly."""
        if self._connected:
            for (group_id, edge_node_id), _ in self._published_metrics.items():
                self._publish_ndeath(group_id, edge_node_id)
 
        self._client.disconnect()
        self._connected = False
        logger.info(f"[DataOpsNode:{self.node_id}] Disconnected from broker.")
 
    def subscribe(
        self,
        topic: Topic,
        qos: QoS = QoS.AT_LEAST_ONCE,
        callback: Optional[Callable[[Self, Message], None]] = None,
    ) -> None:
        """Subscribe to arbitrary Sparkplug topics."""
 
        def wrapped_callback(_client: Client, message: Message) -> None:
            (callback or self._message_callback)(self, message)
 
        self._client.subscribe(topic, qos, wrapped_callback)
        logger.info(f"[DataOpsNode:{self.node_id}] Subscribed to topic: {topic}")
 
    def unsubscribe(self, topic: Topic) -> None:
        """Unsubscribe from topic."""
        self._client.unsubscribe(topic)
        logger.info(f"[DataOpsNode:{self.node_id}] Unsubscribed from topic: {topic}")
 
    def publish(
        self,
        topic: Topic,
        metrics: List[Metric],
        *,
        qos: QoS = QoS.AT_MOST_ONCE,
        retain: bool = False,
    ) -> None:
        """Publish metrics to arbitrary Sparkplug topics and remember them."""
 
        group_id = topic.group_id
        edge_node_id = topic.edge_node_id
        key = (group_id, edge_node_id)
 
        if key not in self._published_metrics:
            # First-time publish for this topic, trigger NBIRTH
            self._published_metrics[key] = {metric.name: metric for metric in metrics}
            self._publish_nbirth(group_id, edge_node_id, metrics)
        else:
            # Update internal metric state
            for metric in metrics:
                self._published_metrics[key][metric.name] = metric
 
            # Publish NDATA update
            payload = NData(
                timestamp=get_current_timestamp(),
                seq=0,  # Simple seq handling; adjust if needed
                metrics=metrics,
            )
            message = Message(topic=topic, payload=payload, qos=qos, retain=retain)
            self._client.publish(message, include_dtypes=True)
            logger.info(f"[DataOpsNode:{self.node_id}] Published NDATA to topic: {topic}")
 
    def rebirth(self, group_id: str, edge_node_id: str) -> None:
        """Manually trigger rebirth (NBIRTH) for a known topic."""
        key = (group_id, edge_node_id)
        metrics = self._published_metrics.get(key)
 
        if metrics:
            self._publish_nbirth(group_id, edge_node_id, metrics.values())
            logger.info(f"[DataOpsNode:{self.node_id}] Triggered rebirth for {group_id}/{edge_node_id}")
        else:
            logger.warning(f"[DataOpsNode:{self.node_id}] No metrics known for {group_id}/{edge_node_id}, cannot rebirth.")
 
    def _publish_nbirth(self, group_id: str, edge_node_id: str, metrics: Iterable[Metric]) -> None:
        """Helper to publish NBIRTH."""
        topic = Topic(
            message_type=MessageType.NBIRTH,
            group_id=group_id,
            edge_node_id=edge_node_id,
        )
        payload = NBirth(
            timestamp=get_current_timestamp(),
            seq=0,  # Simple seq handling; adjust as necessary
            metrics=list(metrics),
        )
        message = Message(topic=topic, payload=payload, qos=QoS.AT_MOST_ONCE, retain=self._retain_birth_certificates)
        self._client.publish(message, include_dtypes=True)
        logger.info(f"[DataOpsNode:{self.node_id}] Published NBIRTH to {group_id}/{edge_node_id}")
 
    def _publish_ndeath(self, group_id: str, edge_node_id: str) -> None:
        """Helper to publish NDEATH."""
        topic = Topic(
            message_type=MessageType.NDEATH,
            group_id=group_id,
            edge_node_id=edge_node_id,
        )
        payload = NDeath(
            timestamp=get_current_timestamp(),
            bd_seq_metric=None,
        )
        message = Message(topic=topic, payload=payload, qos=QoS.AT_MOST_ONCE, retain=False)
        self._client.publish(message)
        logger.info(f"[DataOpsNode:{self.node_id}] Published NDEATH to {group_id}/{edge_node_id}")
 
    @property
    def connected(self) -> bool:
        """Check if node is connected."""
        return self._connected