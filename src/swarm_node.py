import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Set

@dataclass
class PeerNode:
    address: str
    port: int
    last_seen: float
    is_active: bool

class SwarmNode:
    def __init__(self, host: str = '0.0.0.0', port: int = 8000):
        self.host = host
        self.port = port
        self.peers: Dict[str, PeerNode] = {}
        self.connected = False
        self._heartbeat_interval = 30  # seconds

    async def start(self):
        """Initialize the swarm node and start peer discovery"""
        self.connected = True
        await asyncio.gather(
            self._run_heartbeat(),
            self._listen_for_peers()
        )

    async def _run_heartbeat(self):
        """Periodically send heartbeat to all known peers"""
        while self.connected:
            current_time = time.time()
            dead_peers = set()

            for peer_id, peer in self.peers.items():
                if current_time - peer.last_seen > self._heartbeat_interval * 2:
                    peer.is_active = False
                    dead_peers.add(peer_id)
                else:
                    try:
                        await self._send_heartbeat(peer)
                        peer.last_seen = current_time
                    except Exception:
                        peer.is_active = False
                        dead_peers.add(peer_id)

            # Remove dead peers
            for peer_id in dead_peers:
                del self.peers[peer_id]

            await asyncio.sleep(self._heartbeat_interval)

    async def _listen_for_peers(self):
        """Listen for incoming peer connections and heartbeats"""
        server = await asyncio.start_server(
            self._handle_peer_connection,
            self.host,
            self.port
        )

        async with server:
            await server.serve_forever()

    async def _handle_peer_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle incoming peer connections"""
        peer_addr = writer.get_extra_info('peername')
        peer_id = f"{peer_addr[0]}:{peer_addr[1]}"

        if peer_id not in self.peers:
            self.peers[peer_id] = PeerNode(
                address=peer_addr[0],
                port=peer_addr[1],
                last_seen=time.time(),
                is_active=True
            )

        while True:
            try:
                data = await reader.read(100)
                if not data:
                    break

                if data == b'heartbeat':
                    self.peers[peer_id].last_seen = time.time()
                    writer.write(b'ack')
                    await writer.drain()

            except Exception:
                break

        writer.close()
        await writer.wait_closed()

    async def _send_heartbeat(self, peer: PeerNode):
        """Send heartbeat to a specific peer"""
        reader, writer = await asyncio.open_connection(peer.address, peer.port)

        try:
            writer.write(b'heartbeat')
            await writer.drain()

            response = await reader.read(100)
            if response != b'ack':
                raise ConnectionError('Invalid heartbeat response')

        finally:
            writer.close()
            await writer.wait_closed()

    async def stop(self):
        """Stop the swarm node"""
        self.connected = False

    @property
    def active_peers(self) -> Set[str]:
        """Get set of currently active peer IDs"""
        return {peer_id for peer_id, peer in self.peers.items() if peer.is_active}
