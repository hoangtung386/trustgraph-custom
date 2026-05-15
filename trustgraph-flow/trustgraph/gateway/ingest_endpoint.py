
import base64
import logging
import mimetypes
import uuid
import time

from aiohttp import web

from .. schema import LibrarianRequest, LibrarianResponse

logger = logging.getLogger(__name__)


class IngestEndpoint:
    """POST /api/v1/ingest — multipart file upload + auto pipeline trigger.
    GET  /api/v1/ingest/{document_id}/status  — check processing status.
    GET  /api/v1/ingest/{document_id}/graph   — retrieve knowledge graph.
    """

    def __init__(self, endpoint_path, auth, librarian_dispatcher):
        self.path = endpoint_path
        self.auth = auth
        self.librarian_dispatcher = librarian_dispatcher

    async def start(self):
        pass

    def add_routes(self, app):
        app.add_routes([
            web.post(self.path, self.handle_upload),
            web.get(f"{self.path}/{{document_id}}/status", self.handle_status),
            web.get(f"{self.path}/{{document_id}}/graph", self.handle_graph),
        ])

    async def _auth_identity(self, request):
        try:
            return await self.auth.authenticate(request)
        except web.HTTPException:
            raise
        except Exception:
            raise web.HTTPUnauthorized()

    async def _dispatch_librarian(self, body):
        async def noop_responder(x, fin):
            pass
        return await self.librarian_dispatcher.process(
            body, noop_responder, {},
        )

    async def handle_upload(self, request):
        identity = await self._auth_identity(request)
        workspace = getattr(identity, 'workspace', 'default') or 'default'

        try:
            reader = await request.multipart()
        except Exception:
            return web.json_response({"error": "invalid multipart"}, status=400)

        data = {}
        while True:
            field = await reader.next()
            if field is None:
                break
            if field.name == "file":
                data["file"] = field
            else:
                data[field.name] = await field.text()

        if "file" not in data:
            return web.json_response({"error": "missing 'file' field"}, status=400)

        file_field = data["file"]
        filename = file_field.filename or "document"
        content = await file_field.read()

        if not content:
            return web.json_response({"error": "empty file"}, status=400)

        kind = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        title = data.get("title", filename)
        flow = data.get("flow", "")
        collection = data.get("collection", "default")

        doc_id = str(uuid.uuid4())
        proc_id = str(uuid.uuid4())
        now = int(time.time())

        body = {
            "workspace": workspace,
            "operation": "ingest",
            "document-id": doc_id,
            "document-metadata": {
                "id": doc_id,
                "time": now,
                "kind": kind,
                "title": title,
                "comments": "",
                "tags": [],
                "parent-id": "",
                "document-type": "source",
            },
            "content": base64.b64encode(content).decode("utf-8"),
            "processing-metadata": {
                "id": proc_id,
                "document-id": doc_id,
                "time": now,
                "flow": flow,
                "collection": collection,
                "tags": [],
            },
            "collection": collection,
        }

        try:
            resp = await self._dispatch_librarian(body)
        except Exception as e:
            logger.error(f"Ingest failed: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

        return web.json_response({
            "document_id": doc_id,
            "pdf_url": f"/api/v1/document-stream?document-id={doc_id}",
            "status_url": f"/api/v1/ingest/{doc_id}/status",
            "graph_url": f"/api/v1/ingest/{doc_id}/graph",
            "status": "processing",
        })

    async def handle_status(self, request):
        identity = await self._auth_identity(request)
        workspace = getattr(identity, 'workspace', 'default') or 'default'
        document_id = request.match_info.get("document_id", "")

        if not document_id:
            return web.json_response({"error": "missing document_id"}, status=400)

        body = {
            "workspace": workspace,
            "operation": "ingest-status",
            "document-id": document_id,
        }

        try:
            resp = await self._dispatch_librarian(body)
        except Exception as e:
            logger.error(f"Status check failed: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

        if "error" in resp:
            return web.json_response(resp, status=404)

        return web.json_response(resp)

    async def handle_graph(self, request):
        identity = await self._auth_identity(request)
        workspace = getattr(identity, 'workspace', 'default') or 'default'
        document_id = request.match_info.get("document_id", "")

        if not document_id:
            return web.json_response({"error": "missing document_id"}, status=400)

        body = {
            "workspace": workspace,
            "operation": "ingest-graph",
            "document-id": document_id,
        }

        try:
            resp = await self._dispatch_librarian(body)
        except Exception as e:
            logger.error(f"Graph retrieval failed: {e}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

        if "error" in resp:
            return web.json_response(resp, status=404)

        return web.json_response(resp)
