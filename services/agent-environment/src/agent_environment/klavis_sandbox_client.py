"""
Klavis Sandbox MCP Client for connecting to remote Klavis sandbox servers.
Uses StreamableHttp transport to connect to sandbox MCP servers acquired via Klavis API.
"""

import asyncio
import os
import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from typing import Any
from .logger import create_logger

logger = create_logger(__name__)

KLAVIS_API_KEY = os.getenv("KLAVIS_API_KEY", "")

# maps ground truth names to Klavis sandbox names
# Format: {ground_truth_server_name: actual_klavis_sandbox_name}
SERVER_NAME_ALIASES = {
    "osm-mcp-server": "osm",
    "met-museum": "met_museum",
    "clinicaltrialsgov-mcp-server": "clinicaltrialsgov",
    "national-parks": "national_parks",
    "open-library": "open_library",
    "lara-translate": "lara_translate",
    "e2b-server": "e2b",
    "cli-mcp-server": "terminal",
    "memory": "localmemory",
    "weather-data": "weather",
    "weather": "us_weather",
    "google-workspace": "googleworkspaceatlas",
    "mcp-server-code-runner": "code-runner",
    "mcp-code-executor": "code-executor",
}

# Reverse mapping for get_all_server_names to include ground truth server names
REVERSE_SERVER_ALIASES = {v: k for k, v in SERVER_NAME_ALIASES.items()}

# Local sandbox servers acquired via the dedicated /local-sandbox endpoint
# These servers run in a shared local sandbox VM environment
DEFAULT_LOCAL_SANDBOX_SERVERS = [
    "filesystem",
    "git",
    "terminal",
    "desktop-commander",
    "arxiv",
    "code-executor",
    "code-runner",
]

DEFAULT_KLAVIS_MCP_SANDBOXES = [
    # Default servers that don't require API keys
    "calculator",
    "clinicaltrialsgov",
    "us_weather",
    "context7",
    "met_museum",
    "localmemory",
    "open_library",
    "pubmed",
    "wikipedia",
    
    # Optional servers that require API keys
    "weather",
    "twelvedata",
    "national_parks",
    "lara_translate",
    "e2b",
    "alchemy",
    "github",
    "mongodb",
    "googleworkspaceatlas", # as per MCP Atlas, this sandbox includes gmail and google calendar tools
    "airtable",
    "notion",
]


class KlavisSandboxManager:
    KLAVIS_API_URL = "https://api.klavis.ai"

    def __init__(self):
        self.acquired_sandboxes: dict[str, dict] = {}  # server_name -> sandbox info
        self.local_sandbox_id: str | None = None  # ID of the local sandbox environment
        self.local_sandbox_servers: dict[str, str] = {}  # server_name -> mcp_server_url
        self.sessions: dict[str, ClientSession] = {}  # server_name -> MCP session
        self._http_client: httpx.AsyncClient | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                headers={"Authorization": f"Bearer {KLAVIS_API_KEY}"},
                timeout=30.0,
            )
        return self._http_client

    async def acquire_sandbox(self, server_name: str) -> dict:
        """Acquire a sandbox for the given server."""
        url = f"{self.KLAVIS_API_URL}/sandbox/{server_name}"
        body = {"benchmark": "MCP_Atlas"} # with MCP_Atlas benchmark parameter, Klavis can initialize the environment from data_exports/README.md for you!

        logger.info(f"Acquiring Klavis sandbox for {server_name}...")
        response = await self.http_client.post(url, json=body if body else None)
        response.raise_for_status()

        data = response.json()
        self.acquired_sandboxes[server_name] = data
        return data

    async def release_sandbox(self, server_name: str) -> None:
        """Release a sandbox for the given server."""
        sandbox = self.acquired_sandboxes.get(server_name)
        if not sandbox:
            return

        sandbox_id = sandbox.get("sandbox_id")
        url = f"{self.KLAVIS_API_URL}/sandbox/{server_name}/{sandbox_id}"

        try:
            logger.info(f"Releasing Klavis sandbox {sandbox_id} for {server_name}...")
            response = await self.http_client.delete(url)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to release Klavis sandbox {sandbox_id}: {e}")
        finally:
            self.acquired_sandboxes.pop(server_name, None)

    async def acquire_local_sandbox(self) -> dict:
        """Acquire a local sandbox environment via the dedicated /local-sandbox endpoint.
        
        This provisions a shared VM with multiple interconnected MCP servers.
        """
        url = f"{self.KLAVIS_API_URL}/local-sandbox"
        body = {
            "server_names": DEFAULT_LOCAL_SANDBOX_SERVERS.copy(),
            "benchmark": "MCP_Atlas",
        }

        logger.info(f"Acquiring Klavis local sandbox with servers: {body['server_names']}...")
        response = await self.http_client.post(url, json=body)
        response.raise_for_status()

        data = response.json()
        self.local_sandbox_id = data.get("local_sandbox_id")

        # Build server_name -> mcp_server_url mapping from the response
        for server in data.get("servers", []):
            server_name = server.get("server_name")
            mcp_url = server.get("mcp_server_url")
            if server_name and mcp_url:
                self.local_sandbox_servers[server_name] = mcp_url

        logger.info(
            f"Acquired Klavis local sandbox {self.local_sandbox_id} "
            f"with {len(self.local_sandbox_servers)} servers: {list(self.local_sandbox_servers.keys())}"
        )
        return data

    async def release_local_sandbox(self) -> None:
        """Release the local sandbox environment."""
        if not self.local_sandbox_id:
            return

        url = f"{self.KLAVIS_API_URL}/local-sandbox/{self.local_sandbox_id}"
        try:
            logger.info(f"Releasing Klavis local sandbox {self.local_sandbox_id}...")
            response = await self.http_client.delete(url)
            response.raise_for_status()
            logger.info(f"Released Klavis local sandbox {self.local_sandbox_id}")
        except Exception as e:
            logger.error(f"Failed to release Klavis local sandbox {self.local_sandbox_id}: {e}")
        finally:
            self.local_sandbox_id = None
            self.local_sandbox_servers.clear()

    async def acquire_all(self) -> None:
        """Acquire local sandbox and all configured regular sandboxes (in parallel)."""
        servers = DEFAULT_KLAVIS_MCP_SANDBOXES.copy()
        logger.info(
            f"Acquiring Klavis local sandbox + {len(servers)} regular sandbox servers in parallel"
        )

        # Acquire local sandbox and regular sandboxes in parallel
        results = await asyncio.gather(
            self.acquire_local_sandbox(),
            *(self.acquire_sandbox(server) for server in servers),
            return_exceptions=True,
        )

        # Check local sandbox result (first item)
        if isinstance(results[0], Exception):
            logger.error(f"Failed to acquire Klavis local sandbox: {results[0]}")

        # Check regular sandbox results (remaining items)
        for server, result in zip(servers, results[1:]):
            if isinstance(result, Exception):
                logger.error(f"Failed to acquire Klavis sandbox for {server}: {result}")

    async def release_all(self) -> None:
        """Release all acquired sandboxes (local sandbox + regular sandboxes) (in parallel)."""
        # Release local sandbox
        await self.release_local_sandbox()

        # Release regular sandboxes
        servers = list(self.acquired_sandboxes.keys())
        if servers:
            logger.info(f"Releasing {len(servers)} Klavis sandbox servers in parallel: {servers}")
            await asyncio.gather(
                *(self.release_sandbox(server) for server in servers),
                return_exceptions=True
            )

        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    def get_server_url(self, server_name: str) -> str | None:
        """Get the MCP server URL for a server (searches local sandbox + regular sandboxes)."""
        # Check local sandbox servers first
        if server_name in self.local_sandbox_servers:
            return self.local_sandbox_servers[server_name]

        # Check regular sandboxes
        for sandbox in self.acquired_sandboxes.values():
            server_urls = sandbox.get("server_urls", {})
            if server_name in server_urls:
                return server_urls[server_name]
        
        logger.error(f"Server {server_name} not found in any acquired sandbox")
        return None

    def get_all_server_urls(self) -> dict[str, str]:
        """Get all acquired MCP server URLs (local sandbox + regular sandboxes)."""
        urls = {}
        # Add local sandbox server URLs
        urls.update(self.local_sandbox_servers)
        # Add regular sandbox server URLs
        for sandbox in self.acquired_sandboxes.values():
            server_urls = sandbox.get("server_urls", {})
            urls.update(server_urls)
        return urls
    
    def get_all_server_names(self) -> list[str]:
        """Get all server names from acquired sandboxes, normalized to ground truth names."""
        server_names = set()
        # Add local sandbox server names
        for name in self.local_sandbox_servers.keys():
            normalized_name = REVERSE_SERVER_ALIASES.get(name, name)
            server_names.add(normalized_name)
        # Add regular sandbox server names
        for sandbox in self.acquired_sandboxes.values():
            server_urls = sandbox.get("server_urls", {})
            for name in server_urls.keys():
                # Replace with ground truth name if mapping exists
                normalized_name = REVERSE_SERVER_ALIASES.get(name, name)
                server_names.add(normalized_name)
        return sorted(server_names)


class KlavisSandboxMCPClient:
    """MCP Client that connects to Klavis sandbox servers via StreamableHttp transport.
    
    Each call_tool operation connects and disconnects automatically.
    list_tools results are cached since tool schemas don't change.
    """

    def __init__(self, manager: KlavisSandboxManager):
        self.manager = manager
        self._cached_tools: list | None = None

    async def _connect_server(self, server_name: str, url: str) -> tuple[ClientSession, Any]:
        """Connect to a single Klavis sandbox MCP server via StreamableHttp.
        
        Returns (session, exit_stack) for caller to manage cleanup.
        """
        from contextlib import AsyncExitStack

        logger.info(f"Connecting to {server_name} at {url}")
        exit_stack = AsyncExitStack()
        await exit_stack.__aenter__()

        read_stream, write_stream, _ = await exit_stack.enter_async_context(
            streamablehttp_client(url)
        )
        session = await exit_stack.enter_async_context(
            ClientSession(read_stream, write_stream)
        )
        await session.initialize()
        return session, exit_stack

    async def _cleanup(self, exit_stack: Any, server_name: str) -> None:
        """Cleanup a session's exit stack."""
        try:
            await exit_stack.__aexit__(None, None, None)
        except (Exception, asyncio.CancelledError) as e:
            logger.error(f"Error disconnecting from {server_name}: {e}")

    async def list_tools(self) -> list:
        """List all tools from all servers (cached after first call).
        
        Tool naming pattern: {server_name}_{original_tool_name}
        """
        if self._cached_tools is not None:
            logger.debug("Returning cached tools list")
            return self._cached_tools

        all_tools = []
        server_urls = self.manager.get_all_server_urls()

        # Apply REVERSE_SERVER_ALIASES mapping to normalize server names
        normalized_server_urls = {
            REVERSE_SERVER_ALIASES.get(name, name): url
            for name, url in server_urls.items()
        }
        # Sort by normalized server names
        sorted_server_items = sorted(normalized_server_urls.items(), key=lambda x: x[0])
        logger.info(f"Sorted server names: {[name for name, _ in sorted_server_items]}")

        async def _list_tools_from_server(server_name: str, url: str) -> list:
            """Connect to a server, list its tools, and disconnect."""
            session, exit_stack = await self._connect_server(server_name, url)
            try:
                result = await session.list_tools()
                for tool in result.tools:
                    tool.name = f"{server_name}_{tool.name}"
                return result.tools
            finally:
                await self._cleanup(exit_stack, server_name)

        results = await asyncio.gather(
            *(_list_tools_from_server(name, url) for name, url in sorted_server_items),
            return_exceptions=True
        )
        for (server_name, _), result in zip(sorted_server_items, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to list tools from {server_name}: {result}")
            else:
                all_tools.extend(result)

        self._cached_tools = all_tools
        logger.info(f"Cached {len(all_tools)} tools from {len(sorted_server_items)} servers")
        return all_tools

    async def call_tool(self, tool_name: str, arguments: dict) -> Any:
        """Call a tool on the appropriate server, connecting and disconnecting.
        
        Tool name format: {server_name}_{original_tool_name}
        """
        if "_" not in tool_name:
            raise ValueError(f"Invalid tool name format: {tool_name}")

        parts = tool_name.split("_", 1)
        server_name = parts[0]
        actual_tool_name = parts[1]

        # Map aliased server name to actual Klavis server name
        if server_name in SERVER_NAME_ALIASES:
            actual_server = SERVER_NAME_ALIASES[server_name]
            server_name = actual_server

        url = self.manager.get_server_url(server_name)
        if not url:
            available_servers = list(self.manager.get_all_server_urls().keys())
            logger.error(f"No server URL for '{server_name}'. Available: {available_servers}")
            raise ValueError(f"No server URL for: {server_name}")

        session, exit_stack = await self._connect_server(server_name, url)
        try:
            return await session.call_tool(actual_tool_name, arguments)
        except (Exception, asyncio.CancelledError) as e:
            logger.error(f"Tool execution failed - server: '{server_name}', tool: '{actual_tool_name}', error: {e}")
            raise
        finally:
            await self._cleanup(exit_stack, server_name)


# Global manager instance
klavis_sandbox_manager = KlavisSandboxManager()
klavis_sandbox_client = KlavisSandboxMCPClient(klavis_sandbox_manager)
