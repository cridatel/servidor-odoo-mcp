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
        },
        {
            "name": "crear_reserva",
            "description": "Crea una reserva de stock en Odoo para un cliente. Si el cliente no existe, lo crea automáticamente.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sku": {"type": "string", "description": "SKU del producto a reservar."},
                    "cantidad": {"type": "integer", "description": "Cantidad a reservar."},
                    "nombre_cliente": {"type": "string", "description": "Nombre completo o empresa del cliente."},
                    "telefono_cliente": {"type": "string", "description": "Teléfono del cliente (opcional)."},
                    "email_cliente": {"type": "string", "description": "Email del cliente (opcional)."}
                }
            }
        },
        {
            "name": "diagnostico_picking",
            "description": "Diagnóstico: muestra los tipos de operación disponibles en Odoo.",
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

# 6. BUSCAR O CREAR CLIENTE EN ODOO
def buscar_o_crear_cliente(nombre, telefono="", email=""):
    """Busca un cliente por nombre o teléfono. Si no existe, lo crea."""
    
    if telefono:
        domain = [("phone", "=", telefono)]
        cliente = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
            "res.partner", "search_read", [domain],
            {"fields": ["id", "name"], "limit": 1})
        if cliente:
            return cliente[0]["id"]
    
    if nombre:
        domain = [("name", "ilike", nombre)]
        cliente = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
            "res.partner", "search_read", [domain],
            {"fields": ["id", "name"], "limit": 1})
        if cliente:
            return cliente[0]["id"]
    
    vals = {"name": nombre}
    if telefono: vals["phone"] = telefono
    if email: vals["email"] = email
    
    new_id = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
        "res.partner", "create", [vals])
    return new_id

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
            
            all_products = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                "product.product", "search_read", [[]],
                {"fields": fields})
            
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
        
        # ---- HERRAMIENTA DE RESERVA ----
        elif tool_name == "crear_reserva":
            sku = arguments.get("sku", "")
            cantidad = arguments.get("cantidad", 0)
            nombre_cliente = arguments.get("nombre_cliente", "")
            telefono_cliente = arguments.get("telefono_cliente", "")
            email_cliente = arguments.get("email_cliente", "")
            
            # 1. Buscar producto
            domain = [("default_code", "=", sku)]
            product = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                "product.product", "search_read", [domain],
                {"fields": ["id", "name", "qty_available", "list_price"], "limit": 1})
            
            if not product:
                results = {"error": f"No se encontró el SKU: {sku}"}
            elif product[0]["qty_available"] < cantidad:
                results = {"error": f"Stock insuficiente. Disponible: {product[0]['qty_available']}, Solicitado: {cantidad}"}
            else:
                # 2. Buscar o crear cliente
                cliente_id = buscar_o_crear_cliente(nombre_cliente, telefono_cliente, email_cliente)
                
                # 3. Crear albarán de entrega
                picking_id = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                    "stock.picking", "create", [{
                        "partner_id": cliente_id,
                        "picking_type_id": 1,
                        "location_id": 8,
                        "location_dest_id": 5,
                    }])
                
                # 4. Crear movimiento de stock vinculado al albarán
                models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                    "stock.move", "create", [{
                        "product_id": product[0]["id"],
                        "product_uom_qty": cantidad,
                        "product_uom_id": 1,
                        "name": product[0]["name"],
                        "picking_id": picking_id,
                        "location_id": 8,
                        "location_dest_id": 5,
                    }])
                
                # 5. Confirmar el albarán
                models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                    "stock.picking", "action_confirm", [[picking_id]])
                
                results = {
                    "success": True,
                    "albaran_id": picking_id,
                    "cliente": nombre_cliente,
                    "producto": product[0]["name"],
                    "sku": sku,
                    "cantidad": cantidad,
                    "precio_unitario": product[0]["list_price"],
                    "total": round(cantidad * product[0]["list_price"], 2),
                    "estado": "Reserva creada. Stock separado para el cliente."
                }
        
        # ---- HERRAMIENTA DE DIAGNÓSTICO ----
        elif tool_name == "diagnostico_picking":
            picking_types = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
                "stock.picking.type", "search_read", [[]],
                {"fields": ["id", "name", "code"]})
            results = picking_types
        
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
