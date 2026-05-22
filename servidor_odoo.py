import os
import json
import xmlrpc.client
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport

# 1. CONFIGURACIÓN DE ODOO
ODOO_URL = os.environ["ODOO_URL"]
ODOO_DB = os.environ["ODOO_DB"]
ODOO_USER = os.environ["ODOO_USER"]
ODOO_PASSWORD = os.environ["ODOO_PASSWORD"]

common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASSWORD, {})
models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")

# 2. CREAR SERVIDOR MCP
mcp = FastMCP(name="Odoo Inventory Server")

# 3. HERRAMIENTA DE BÚSQUEDA
@mcp.tool()
def buscar_productos_odoo(keyword: str = "", limite: int = 5) -> str:
    """Busca productos en Odoo."""
    domain = [("name", "ilike", keyword)] if keyword else []
    fields = ["name", "default_code", "qty_available"]
    results = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
        "product.product", "search_read", [domain],
        {"fields": fields, "limit": limite})
    return json.dumps(results, indent=2, ensure_ascii=False)

# 4. APP FASTAPI
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

sse = SseServerTransport("/messages")

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/sse")
async def handle_sse(request: Request):
    async def generator():
        async with sse.connect_sse(
            request.scope, request.receive, request._send
        ) as streams:
            await mcp._mcp_server.run(
                streams[0], streams[1],
                mcp._mcp_server.create_initialization_options()
            )
    return StreamingResponse(generator(), media_type="text/event-stream")

@app.post("/messages")
async def handle_messages(request: Request):
    await sse.handle_message(request.scope, request.receive, request._send)
    return {"status": "ok"}
