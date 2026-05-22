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
    "tools": [{
        "name": "buscar_productos_odoo",
        "description": "Busca productos en Odoo por nombre y devuelve nombre, SKU y stock disponible.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Palabra clave para filtrar productos."},
                "limite": {"type": "integer", "description": "Número máximo de productos a devolver.", "default": 5}
            }
        }
    }]
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
            # Enviar endpoint del mensaje
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
    
    # Ignorar notificaciones
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
        keyword = params.get("arguments", {}).get("keyword", "")
        limite = params.get("arguments", {}).get("limite", 5)
        
        domain = [("name", "ilike", keyword)] if keyword else []
        fields = ["name", "default_code", "qty_available"]
        results = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
            "product.product", "search_read", [domain],
            {"fields": fields, "limit": limite})
        
        respuesta = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "content": [{"type": "text", "text": json.dumps(results, indent=2, ensure_ascii=False)}]
            }
        }
    
    else:
        respuesta = {"jsonrpc": "2.0", "id": msg_id, "error": {"code": -32601, "message": "Method not found"}}
    
    # Enviar respuesta a través de la cola SSE
    respuesta_str = json.dumps(respuesta)
    if session_id in sessions:
        await sessions[session_id].put(respuesta_str)
    
    print("=== RESPUESTA ===")
    print(respuesta_str)
    return {"status": "ok"}

@app.get("/")
def root():
    return {"status": "ok", "tools": TOOLS}
