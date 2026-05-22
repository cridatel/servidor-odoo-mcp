import os
import json
import xmlrpc.client
import asyncio
import uuid
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

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

# 3. ENDPOINT SSE SIMPLE
@app.get("/sse")
async def sse():
    async def generator():
        # Enviar evento inicial
        session_id = str(uuid.uuid4())
        yield f"event: endpoint\ndata: /messages?session_id={session_id}\n\n"
        
        # Mantener conexión viva
        while True:
            await asyncio.sleep(15)
            yield ": keepalive\n\n"
    
    return StreamingResponse(generator(), media_type="text/event-stream",
                           headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

# 4. HERRAMIENTA DE BÚSQUEDA (vía POST)
@app.post("/messages")
async def messages(request: Request):
    body = await request.json()
    method = body.get("method", "")
    
    if method == "tools/list":
        return {
            "tools": [{
                "name": "buscar_productos_odoo",
                "description": "Busca productos en Odoo por nombre y devuelve nombre, SKU y stock disponible.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "keyword": {"type": "string", "description": "Palabra clave para filtrar productos."},
                        "limite": {"type": "integer", "description": "Número máximo de productos a devolver."}
                    }
                }
            }]
        }
    
    elif method == "tools/call":
        params = body.get("params", {})
        keyword = params.get("arguments", {}).get("keyword", "")
        limite = params.get("arguments", {}).get("limite", 5)
        
        domain = [("name", "ilike", keyword)] if keyword else []
        fields = ["name", "default_code", "qty_available"]
        results = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD,
            "product.product", "search_read", [domain],
            {"fields": fields, "limit": limite})
        
        return {"content": [{"type": "text", "text": json.dumps(results, indent=2, ensure_ascii=False)}]}
    
    return {"error": "Unknown method"}

@app.get("/")
def root():
    return {"status": "ok"}
