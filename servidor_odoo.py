import os
import json
import xmlrpc.client
import asyncio
import uuid
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import Dict, Any

# 1. CONFIGURACIÓN DE ODOO
ODOO_URL = os.environ["ODOO_URL"]
ODOO_DB = os.environ["ODOO_DB"]
ODOO_USER = os.environ["ODOO_USER"]
ODOO_PASSWORD = os.environ["ODOO_PASSWORD"]

common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASSWORD, {})
models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")

# 2. APP FASTAPI
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# 3. HERRAMIENTAS DISPONIBLES
TOOLS = {
    "tools": [
        {
            "name": "buscar_productos",
            "description": "Busca productos en Odoo por nombre, SKU, categoría o stock mínimo.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "keyword": {"type": "string", "description": "Palabra clave para buscar por nombre del producto."},
                    "sku": {"type": "string", "description": "SKU o referencia interna del producto (ej: EMB-007)."},
                    "categoria": {"type": "string", "description": "Nombre de la categoría (ej: Embalaje, Protección)."},
                    "stock_minimo": {"type": "integer", "description": "Filtrar productos con stock mayor o igual a este valor."},
                    "limite": {"type": "integer", "description": "Número máximo de productos a devolver.", "default": 20}
                }
            }
        },
        {
            "name": "consultar_stock",
            "description": "Consulta el stock detallado de un producto específico por su SKU exacto.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sku": {"type": "string", "description": "SKU exacto del producto (ej: EMB-007)."}
                }
            }
        },
        {
            "name": "productos_agotados",
            "description": "Lista todos los productos que están agotados (stock = 0).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limite": {"type": "integer", "description": "Número máximo de productos a devolver.", "default": 50}
                }
            }
        },
        {
            "name": "productos_stock_bajo",
            "description": "Lista todos los productos con stock bajo (entre 1 y 10 unidades).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limite": {"type": "integer", "description": "Número máximo de productos a devolver.", "default": 50}
                }
            }
        },
        {
            "name": "buscar_por_precio",
            "description": "Busca productos filtrando por precio máximo.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "precio_maximo": {"type": "number", "description": "Precio máximo en euros (ej: 5.00)."},
                    "categoria": {"type": "string", "description": "Opcional: filtrar también por categoría."},
                    "limite": {"type": "integer", "description": "Número máximo de productos a devolver.", "default": 20}
                }
            }
        },
        {
            "name": "resumen_inventario",
            "description": "Muestra un resumen general del inventario: total de productos, stock total y valor total.",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        }
    ]
}

# 4. COLA DE MENSAJES POR SESIÓN
sessions: Dict[str, asyncio.Queue] = {}

# 5. ENDPOINT SSE
@app.get("/sse")
async def sse(request: Request):
    session_id = request.query_params.get("session_id", str(uuid.uuid4()))
    queue: asyncio.Queue = asyncio.Queue()
    sessions[session_id] = queue
    
    async def generator():
        try:
            yield f"event: endpoint\ndata: /messages?session_id={session_id}\n\n"
            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"event: message\ndata: {message}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            sessions.pop(session_id, None)
    
    return StreamingResponse(generator(), media_type="text/event-stream",
                           headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

# 6. ENDPOINT MENSAJES
@app.post("/messages")
async def messages(request: Request):
    body = await request.json()
    session_id = request.query_params.get("session_id", "")
    
    print("=== MEETIP360 ENVIA ===")
    print(json.dumps(body, indent=2))
    
    method = body.get("method", "")
    msg_id = body.get("id", None)
    
    if msg_id is None:
        print("=== NOTIFICACION (ignorada) ===")
        return {}
    
    respuesta = None
    
    if method == "initialize":
        respuesta = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "Odoo Inventory Server", "version": "1.0.0"}
            }
        }
    
    elif method == "tools/list":
        respuesta = {"jsonrpc": "2.0", "id": msg_id, "result": TOOLS}
    
    elif method == "tools/call":
        params = body.get("params", {})
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        fields = ["name", "default_code", "qty_available", "categ_id", "list_price"]
        
        if tool_name == "buscar_productos":
            keyword = arguments.get("keyword", "")
            sku = arguments.get("sku", "")
            categoria = arguments.get("categoria", "")
            stock_minimo = arguments.get("stock_minimo", None)
            limite = arguments.get("limite", 20)
            
            domain = []
            if keyword: domain.append(("name", "ilike", keyword))
            if sku: domain.append(("default_code", "ilike", sku))
            if categoria: domain.append(("categ_id.name", "ilike", categoria))
            if stock_minimo is not None: domain.append(("qty_available", ">=", stock_minimo))
            
            results = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                "product.product", "search_read", [domain],
                {"fields": fields, "limit": limite})
        
        elif tool_name == "consultar_stock":
            sku = arguments.get("sku", "")
            domain = [("default_code", "=", sku)]
            results = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                "product.product", "search_read", [domain],
                {"fields": fields, "limit": 1})
            if not results:
                results = {"error": f"No se encontró ningún producto con SKU: {sku}"}
        
        elif tool_name == "productos_agotados":
            limite = arguments.get("limite", 50)
            domain = [("qty_available", "=", 0)]
            results = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                "product.product", "search_read", [domain],
                {"fields": fields, "limit": limite})
        
        elif tool_name == "productos_stock_bajo":
            limite = arguments.get("limite", 50)
            domain = [("qty_available", ">", 0), ("qty_available", "<=", 10)]
            results = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                "product.product", "search_read", [domain],
                {"fields": fields, "limit": limite})
        
        elif tool_name == "buscar_por_precio":
            precio_maximo = arguments.get("precio_maximo", 0)
            categoria = arguments.get("categoria", "")
            limite = arguments.get("limite", 20)
            
            domain = [("list_price", "<=", precio_maximo)]
            if categoria: domain.append(("categ_id.name", "ilike", categoria))
            
            results = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                "product.product", "search_read", [domain],
                {"fields": fields, "limit": limite})
        
        elif tool_name == "resumen_inventario":
            # Obtener todos los productos
            all_products = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                "product.product", "search_read", [[]],
                {"fields": ["qty_available", "list_price"]})
            
            total_productos = len(all_products)
            total_stock = sum(p["qty_available"] for p in all_products)
            valor_total = sum(p["qty_available"] * p["list_price"] for p in all_products)
            agotados = sum(1 for p in all_products if p["qty_available"] == 0)
            stock_bajo = sum(1 for p in all_products if 0 < p["qty_available"] <= 10)
            
            results = {
                "total_productos": total_productos,
                "total_stock": total_stock,
                "valor_total_inventario": round(valor_total, 2),
                "productos_agotados": agotados,
                "productos_stock_bajo": stock_bajo,
                "productos_disponibles": total_productos - agotados
            }
        
        else:
            results = {"error": f"Herramienta no encontrada: {tool_name}"}
        
        respuesta = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "content": [{"type": "text", "text": json.dumps(results, indent=2, ensure_ascii=False)}]
            }
        }
    
    else:
        respuesta = {"jsonrpc": "2.0", "id": msg_id, "error": {"code": -32601, "message": "Method not found"}}
    
    respuesta_str = json.dumps(respuesta)
    if session_id in sessions:
        await sessions[session_id].put(respuesta_str)
    
    print("=== RESPUESTA ===")
    print(respuesta_str)
    return {"status": "ok"}

@app.get("/")
def root():
    return {"status": "ok", "total_tools": len(TOOLS["tools"])}
