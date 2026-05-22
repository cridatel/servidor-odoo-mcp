import os
import json
import xmlrpc.client
import asyncio
import uuid
from datetime import datetime, timedelta
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
            "description": "Busca productos por nombre, SKU, categoría o stock mínimo.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "keyword": {"type": "string", "description": "Palabra clave para buscar por nombre."},
                    "sku": {"type": "string", "description": "SKU del producto."},
                    "categoria": {"type": "string", "description": "Nombre de la categoría."},
                    "stock_minimo": {"type": "integer", "description": "Filtrar con stock mayor o igual a este valor."},
                    "limite": {"type": "integer", "description": "Máximo de productos.", "default": 20}
                }
            }
        },
        {
            "name": "consultar_stock",
            "description": "Consulta el stock detallado de un producto por su SKU exacto.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sku": {"type": "string", "description": "SKU exacto del producto."}
                }
            }
        },
        {
            "name": "productos_agotados",
            "description": "Lista todos los productos agotados (stock = 0).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limite": {"type": "integer", "default": 50}
                }
            }
        },
        {
            "name": "productos_stock_bajo",
            "description": "Lista productos con stock bajo (1 a 10 unidades).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limite": {"type": "integer", "default": 50}
                }
            }
        },
        {
            "name": "buscar_por_precio",
            "description": "Busca productos filtrando por precio máximo.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "precio_maximo": {"type": "number", "description": "Precio máximo en euros."},
                    "categoria": {"type": "string", "description": "Filtrar también por categoría."},
                    "limite": {"type": "integer", "default": 20}
                }
            }
        },
        {
            "name": "resumen_inventario",
            "description": "Muestra resumen general: total productos, stock y valor.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "actualizar_stock",
            "description": "Actualiza el stock de un producto (entrada o salida).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sku": {"type": "string", "description": "SKU del producto a modificar."},
                    "cantidad": {"type": "integer", "description": "Cantidad a añadir (positivo) o quitar (negativo)."},
                    "motivo": {"type": "string", "description": "Motivo del cambio (ej: recepción, venta, ajuste)."}
                }
            }
        },
        {
            "name": "crear_producto",
            "description": "Crea un nuevo producto en el inventario.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "nombre": {"type": "string", "description": "Nombre del producto."},
                    "sku": {"type": "string", "description": "SKU o referencia interna."},
                    "categoria": {"type": "string", "description": "Nombre de la categoría."},
                    "precio": {"type": "number", "description": "Precio de venta en euros."},
                    "stock_inicial": {"type": "integer", "description": "Stock inicial.", "default": 0}
                }
            }
        },
        {
            "name": "top_productos",
            "description": "Muestra los productos con más o menos stock.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "tipo": {"type": "string", "description": "'mayor' para más stock, 'menor' para menos stock.", "default": "mayor"},
                    "limite": {"type": "integer", "description": "Cantidad de productos a mostrar.", "default": 5}
                }
            }
        },
        {
            "name": "valor_por_categoria",
            "description": "Muestra el valor del inventario desglosado por categoría.",
            "inputSchema": {"type": "object", "properties": {}}
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

# 6. OBTENER ID DE CATEGORÍA POR NOMBRE
def get_category_id(name):
    results = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
        "product.category", "search_read", [[("name", "ilike", name)]],
        {"fields": ["id"], "limit": 1})
    return results[0]["id"] if results else None

# 7. ENDPOINT MENSAJES
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
                "serverInfo": {"name": "Odoo Inventory Server", "version": "2.0.0"}
            }
        }
    
    elif method == "tools/list":
        respuesta = {"jsonrpc": "2.0", "id": msg_id, "result": TOOLS}
    
    elif method == "tools/call":
        params = body.get("params", {})
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        fields = ["name", "default_code", "qty_available", "categ_id", "list_price"]
        
        # ---- HERRAMIENTAS DE BÚSQUEDA ----
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
                results = {"error": f"No se encontró el SKU: {sku}"}
        
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
        
        # ---- HERRAMIENTAS DE ANÁLISIS ----
        elif tool_name == "resumen_inventario":
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
        
        elif tool_name == "top_productos":
            tipo = arguments.get("tipo", "mayor")
            limite = arguments.get("limite", 5)
            
            # Obtener todos los productos (sin order, ya que qty_available no es almacenado)
            all_products = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                "product.product", "search_read", [[]],
                {"fields": fields})
            
            # Ordenar manualmente en Python
            if tipo == "mayor":
                all_products.sort(key=lambda p: p["qty_available"], reverse=True)
            else:
                all_products.sort(key=lambda p: p["qty_available"])
            
            results = all_products[:limite]
        
        elif tool_name == "valor_por_categoria":
            all_products = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                "product.product", "search_read", [[]],
                {"fields": ["categ_id", "qty_available", "list_price"]})
            
            categorias = {}
            for p in all_products:
                cat_name = p["categ_id"][1] if p["categ_id"] else "Sin categoría"
                if cat_name not in categorias:
                    categorias[cat_name] = {"productos": 0, "stock": 0, "valor": 0}
                categorias[cat_name]["productos"] += 1
                categorias[cat_name]["stock"] += p["qty_available"]
                categorias[cat_name]["valor"] += p["qty_available"] * p["list_price"]
            
            results = [{"categoria": k, **v, "valor": round(v["valor"], 2)} for k, v in categorias.items()]
        
        # ---- HERRAMIENTAS DE MODIFICACIÓN ----
        elif tool_name == "actualizar_stock":
            sku = arguments.get("sku", "")
            cantidad = arguments.get("cantidad", 0)
            motivo = arguments.get("motivo", "Ajuste manual")
            
            # Buscar producto
            domain = [("default_code", "=", sku)]
            product = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                "product.product", "search_read", [domain],
                {"fields": ["id", "name", "qty_available"], "limit": 1})
            
            if not product:
                results = {"error": f"No se encontró el SKU: {sku}"}
            else:
                product_id = product[0]["id"]
                stock_anterior = product[0]["qty_available"]
                nuevo_stock = stock_anterior + cantidad
                
                if nuevo_stock < 0:
                    results = {"error": f"Stock insuficiente. Stock actual: {stock_anterior}, intentas quitar: {abs(cantidad)}"}
                else:
                    # Actualizar stock
                    models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                        "product.product", "write", [[product_id], {"qty_available": nuevo_stock}])
                    
                    results = {
                        "success": True,
                        "sku": sku,
                        "producto": product[0]["name"],
                        "stock_anterior": stock_anterior,
                        "cambio": cantidad,
                        "stock_nuevo": nuevo_stock,
                        "motivo": motivo
                    }
        
        elif tool_name == "crear_producto":
            nombre = arguments.get("nombre", "")
            sku = arguments.get("sku", "")
            categoria = arguments.get("categoria", "")
            precio = arguments.get("precio", 0)
            stock_inicial = arguments.get("stock_inicial", 0)
            
            cat_id = get_category_id(categoria) if categoria else None
            
            vals = {
                "name": nombre,
                "default_code": sku,
                "list_price": precio,
                "type": "consu",
            }
            if cat_id:
                vals["categ_id"] = cat_id
            
            new_id = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                "product.product", "create", [vals])
            
            # Si hay stock inicial, actualizarlo después de crear
            if stock_inicial > 0:
                models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                    "product.product", "write", [[new_id], {"qty_available": stock_inicial}])
            
            results = {
                "success": True,
                "id": new_id,
                "nombre": nombre,
                "sku": sku,
                "precio": precio,
                "stock_inicial": stock_inicial,
                "categoria": categoria or "Sin categoría"
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
