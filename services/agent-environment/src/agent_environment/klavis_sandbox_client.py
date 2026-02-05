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

DEFAULT_KLAVIS_MCP_SANDBOXES = [
    # Default servers that don't require API keys
    "local_dev", # Note: local_dev sandbox will return filesystem/git/terminal/desktop-commander/arxiv/excel/word/powerpoint/mcp-code-executor/mcp-server-code-runner remote mcp servers
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
    
    # "notion",
    # "slack",
]


class KlavisSandboxManager:
    KLAVIS_API_URL = "https://api.klavis.ai"

    def __init__(self):
        self.acquired_sandboxes: dict[str, dict] = {}  # server_name -> sandbox info
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
            logger.info(f"Released Klavis sandbox {sandbox_id}")
        except Exception as e:
            logger.error(f"Failed to release Klavis sandbox {sandbox_id}: {e}")
        finally:
            self.acquired_sandboxes.pop(server_name, None)

    async def acquire_all(self) -> None:
        """Acquire sandboxes for all configured servers (in parallel)."""
        servers = DEFAULT_KLAVIS_MCP_SANDBOXES.copy()
        logger.info(f"Acquiring {len(servers)} Klavis sandbox servers in parallel: {servers}")

        results = await asyncio.gather(
            *(self.acquire_sandbox(server) for server in servers),
            return_exceptions=True
        )
        for server, result in zip(servers, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to acquire Klavis sandbox for {server}: {result}")

    async def release_all(self) -> None:
        """Release all acquired sandboxes."""
        servers = list(self.acquired_sandboxes.keys())
        for server in servers:
            await self.release_sandbox(server)

        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    def get_server_url(self, server_name: str) -> str | None:
        """Get the MCP server URL for a server (searches all acquired sandboxes)."""
        for sandbox in self.acquired_sandboxes.values():
            server_urls = sandbox.get("server_urls", {})
            if server_name in server_urls:
                return server_urls[server_name]
        
        logger.error(f"Server {server_name} not found in any acquired sandbox")
        return None

    def get_all_server_urls(self) -> dict[str, str]:
        """Get all acquired MCP server URLs."""
        urls = {}
        for sandbox in self.acquired_sandboxes.values():
            server_urls = sandbox.get("server_urls", {})
            # Add all server URLs from this sandbox
            urls.update(server_urls)
        return urls
    
    def get_all_server_names(self) -> list[str]:
        """Get all server names from acquired sandboxes, normalized to ground truth names."""
        server_names = set()
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

        for server_name, url in server_urls.items():
            try:
                session, exit_stack = await self._connect_server(server_name, url)
                try:
                    result = await session.list_tools()
                    prefix = REVERSE_SERVER_ALIASES.get(server_name, server_name)
                    for tool in result.tools:
                        tool.name = f"{prefix}_{tool.name}"
                    all_tools.extend(result.tools)
                finally:
                    await self._cleanup(exit_stack, server_name)
            except (Exception, asyncio.CancelledError) as e:
                logger.error(f"Failed to list tools from {server_name}: {e}")

        self._cached_tools = all_tools
        logger.info(f"Cached {len(all_tools)} tools from {len(server_urls)} servers")
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
