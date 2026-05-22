import os
import json
import xmlrpc.client
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport
import asyncio

# 1. CONFIGURACIÓN DE ODOO
ODOO_URL = os.environ["ODOO_URL"]
ODOO_DB = os.environ["ODOO_DB"]
ODOO_USER = os.environ["ODOO_USER"]
ODOO_PASSWORD = os.environ["ODOO_PASSWORD"]

common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASSWORD, {})
models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")

# 2. CREAR SERVIDOR MCP
mcp = FastMCP(
    name="Odoo Inventory Server",
    instructions="Servidor MCP para consultar el inventario de Odoo en tiempo real."
)

# 3. HERRAMIENTA DE BÚSQUEDA
@mcp.tool()
def buscar_productos_odoo(keyword: str = "", limite: int = 5) -> str:
    """Busca productos en Odoo por nombre y devuelve nombre, SKU y stock disponible."""
    domain = [("name", "ilike", keyword)] if keyword else []
    fields = ["name", "default_code", "qty_available"]
    results = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        "product.product", "search_read",
        [domain],
        {"fields": fields, "limit": limite}
    )
    return json.dumps(results, indent=2, ensure_ascii=False)

# 4. CREAR APP FASTAPI CON ENDPOINTS SSE MANUALES
app = FastAPI(title="Odoo MCP Server")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

sse_transport = SseServerTransport("/messages")

@app.get("/")
def root():
    return {"status": "ok", "message": "Odoo MCP Server running"}

@app.get("/sse")
async def sse_endpoint():
    """Endpoint SSE que MeetIP360 necesita."""
    async def event_generator():
        async with sse_transport.connect_sse() as streams:
            await mcp._mcp_server.run(streams[0], streams[1], mcp._mcp_server.create_initialization_options())
            async for event in streams[0]:
                yield f"data: {event.model_dump_json()}\n\n"
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.post("/messages")
async def messages_endpoint(request: Request):
    """Endpoint para recibir mensajes del cliente."""
    body = await request.body()
    await sse_transport.handle_message(body)
    return {"status": "ok"}
