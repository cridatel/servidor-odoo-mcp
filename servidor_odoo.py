import os
import json
import xmlrpc.client
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from mcp.server.fastmcp import FastMCP

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

# 4. CREAR APP FASTAPI Y MONTAR MCP
app = FastAPI(title="Odoo MCP Server")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Montar la app MCP en la ruta /mcp
mcp_app = mcp.sse_app()
app.mount("/mcp", mcp_app)

@app.get("/")
def root():
    return {"status": "ok", "message": "Odoo MCP Server running"}
