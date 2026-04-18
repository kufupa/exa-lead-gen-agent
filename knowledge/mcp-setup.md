# Exa MCP Setup

## Prerequisites

This agent requires an Exa API key. Get yours at: https://dashboard.exa.ai/api-keys

## MCP Configuration

Add this to your MCP config before running the agent:

```bash
claude mcp add --transport http exa "https://mcp.exa.ai/mcp?tools=web_search_advanced_exa&exaApiKey=YOUR_EXA_API_KEY"
```

Or add manually to your MCP config JSON:

```json
{
  "servers": {
    "exa": {
      "type": "http",
      "url": "https://mcp.exa.ai/mcp?tools=web_search_advanced_exa&exaApiKey=YOUR_EXA_API_KEY"
    }
  }
}
```

Restart Claude Code after adding the MCP server.

## Exa Documentation

Full docs: https://exa.ai/docs/llms.txt
