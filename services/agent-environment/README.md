# Agent Environment

A Docker container with ~40 pre-configured Model Context Protocol (MCP) servers for AI agents.

## Operating Modes

The agent environment supports two operating modes:

### 1. Klavis Sandbox Mode (Recommended)

Connects to remote MCP servers hosted by [Klavis](https://klavis.ai). This is the recommended mode as it:
- Requires minimal setup (just one API key)
- Automatically provisions and manages remote MCP sandboxes
- Handles environment initialization for MCP Atlas benchmark automatically

**Quick Start with Klavis Mode:**

```bash
# In your .env file:
KLAVIS_SANDBOX_MODE=true
KLAVIS_API_KEY=your_klavis_api_key
```

Then run:
```bash
make build
make run-docker
```

### 2. Local Mode

Runs MCP servers locally inside the Docker container. This mode gives you full control over server configuration but requires more setup.

**Quick Start with Local Mode:**

1. **Set up environment variables:**
   Copy `env.template` to `.env` at the root level directory.

   Setup the API keys for the MCP servers you want to use.
   
   For quick start, modify `.env` to set a few servers that don't need API keys:
   ```bash
   ENABLED_SERVERS=calculator,wikipedia,filesystem,git,fetch
   ```

2. **Run the container:**

   The preferred way is with Docker (run these commands from the root level directory):
   ```bash
   make build
   make run-docker
   ```

   If you don't want to use docker, you can do `make run`, but you'll need to fix the configs that reference the `/data` dir in `src/agent_environment/mcp_server_template.json` and change that to the actual location of the ./data folder on your computer.
   ```bash
   # First, fix references to /data dir in mcp_server_template.json
   make run
   ```

   The container takes 1-3 minutes to start up depending on the number of MCP servers enabled.

## API Endpoints

Once ready, the service provides HTTP endpoints on port 1984:

- `POST /list-tools` - List all available tools from MCP servers
- `POST /call-tool` - Call a specific tool with arguments
- `GET /enabled-servers` - Get list of enabled servers and their status
- `GET /health` - Health check endpoint
- `GET /cache-stats` - Get cache statistics
- `POST /cache-clear` - Clear the response cache

**Test it's working:**
```bash
./dev_scripts/debug_and_concurrency_tests/curl_scripts/mcp__list_tools.sh
./dev_scripts/debug_and_concurrency_tests/curl_scripts/mcp_git.sh
```

## Available MCP Servers

### Klavis Sandbox Mode

Default servers acquired from Klavis (configured in `klavis_sandbox_client.py`):

- **No API keys needed:** calculator, clinicaltrialsgov, us_weather, context7, met_museum, localmemory, open_library, osm, pubmed, wikipedia
- **local_dev sandbox:** Includes filesystem, git, terminal, desktop-commander, arxiv, excel, word, powerpoint, mcp-code-executor, mcp-server-code-runner
- **API keys required (configured in Klavis):** weather, twelvedata, national_parks, lara_translate, e2b, alchemy, github, mongodb, googleworkspaceatlas, airtable

### Local Mode

This project includes 36 MCP servers as configured in `src/agent_environment/mcp_server_template.json`. Some require API keys.

- **No API keys needed:** calculator, wikipedia, filesystem, git, fetch, arxiv, f1-mcp-server, etc.
- **API keys required:** GitHub, Google Maps, Slack, Reddit, Weather, YouTube, and others

See `env.template` for basic information about each API key and where to get it. And see `data_exports/README.md` for info on how to upload data to online services.

## Server Selection (Local Mode Only)

To run only specific servers (useful for testing without API keys):

```bash
# In your .env file:
ENABLED_SERVERS=calculator,wikipedia,filesystem,git,fetch
```

## API Keys

- **Klavis Mode:** Get your API key from [Klavis](https://klavis.ai)
- **Local Mode:** Check `env.template` for all available API key configurations

## Implementation Details

- **Local Mode:** Depends on https://github.com/jlowin/fastmcp
- **Klavis Mode:** Uses StreamableHttp transport to connect to remote Klavis sandbox MCP servers

By default, caching is enabled. If a request is successful, it will be cached, and subsequent identical requests will return the cached value.

At high throughputs, some of the MCP servers may not perform as well or may freeze up. Replicas are recommended for high throughput.
