"""
Marker-pdf decoder for PDF documents.
Converts PDF pages to Markdown using marker-pdf, preserving
headings, tables, lists, images, and formatting.

Supports both inline document data and fetching from librarian via Pulsar
for large documents.

Environment variable:
    MARKER_LANGUAGES   Comma-separated language codes (default: eng)
"""

import base64
import logging
import os

from ...schema import Document, TextDocument, Metadata
from ...schema import Triples
from ...base import FlowProcessor, ConsumerSpec, ProducerSpec, LibrarianSpec

from ...provenance import (
    document_uri,
    page_uri as make_page_uri,
    derived_entity_triples,
    set_graph,
    GRAPH_SOURCE,
)

PdfConverter = None
load_all_models = None
text_from_rendered = None

COMPONENT_NAME = "marker-ocr-decoder"
COMPONENT_VERSION = "1.0.0"

logger = logging.getLogger(__name__)

default_ident = "document-decoder"

default_languages = os.getenv("MARKER_LANGUAGES", "eng")


class Processor(FlowProcessor):
    def __init__(self, **params):

        id = params.get("id", default_ident)
        languages = params.get("languages", default_languages)

        super(Processor, self).__init__(
            **params
            | {
                "id": id,
            }
        )

        global PdfConverter, load_all_models, text_from_rendered
        if PdfConverter is None:
            from marker.converters.pdf import PdfConverter as _PdfConverter
            from marker.models import load_all_models as _load_all_models
            from marker.output import text_from_rendered as _text_from_rendered

            PdfConverter = _PdfConverter
            load_all_models = _load_all_models
            text_from_rendered = _text_from_rendered

        self.models = load_all_models()
        self.langs = [lang.strip() for lang in languages.split(",")]
        self.converter = PdfConverter(
            artifact_dict=self.models,
            langs=self.langs,
        )

        self.register_specification(
            ConsumerSpec(
                name="input",
                schema=Document,
                handler=self.on_message,
            )
        )

        self.register_specification(
            ProducerSpec(
                name="output",
                schema=TextDocument,
            )
        )

        self.register_specification(
            ProducerSpec(
                name="triples",
                schema=Triples,
            )
        )

        self.register_specification(LibrarianSpec())

        logger.info(f"Marker-pdf processor initialized (langs: {self.langs})")

    async def on_message(self, msg, consumer, flow):

        logger.info("PDF message received")

        v = msg.value()

        logger.info(f"Decoding {v.metadata.id}...")

        if v.document_id:
            doc_meta = await flow.librarian.fetch_document_metadata(
                document_id=v.document_id,
            )
            if doc_meta and doc_meta.kind and doc_meta.kind != "application/pdf":
                logger.error(
                    f"Unsupported MIME type: {doc_meta.kind}. "
                    f"Marker-pdf decoder only handles application/pdf. "
                    f"Ignoring document {v.metadata.id}."
                )
                return

        if v.document_id:
            logger.info(f"Fetching document {v.document_id} from librarian...")
            content = await flow.librarian.fetch_document_content(
                document_id=v.document_id,
            )
            if isinstance(content, str):
                content = content.encode("utf-8")
            blob = base64.b64decode(content)
            logger.info(f"Fetched {len(blob)} bytes from librarian")
        else:
            blob = base64.b64decode(v.data)

        source_doc_id = v.document_id or v.metadata.id

        rendered = self.converter(blob)
        markdown_text, extracted_images, metadata = text_from_rendered(rendered)

        pages = markdown_text.split("\n\f\n")

        for ix, page_text in enumerate(pages):
            page_num = ix + 1

            if not page_text.strip():
                continue

            logger.debug(f"Processing page {page_num}")

            pg_uri = make_page_uri()
            page_doc_id = pg_uri
            page_content = page_text.encode("utf-8")

            await flow.librarian.save_child_document(
                doc_id=page_doc_id,
                parent_id=source_doc_id,
                content=page_content,
                document_type="page",
                title=f"Page {page_num}",
            )

            doc_uri = document_uri(source_doc_id)

            prov_triples = derived_entity_triples(
                entity_uri=pg_uri,
                parent_uri=doc_uri,
                component_name=COMPONENT_NAME,
                component_version=COMPONENT_VERSION,
                label=f"Page {page_num}",
                page_number=page_num,
            )

            await flow("triples").send(
                Triples(
                    metadata=Metadata(
                        id=pg_uri,
                        root=v.metadata.root,
                        collection=v.metadata.collection,
                    ),
                    triples=set_graph(prov_triples, GRAPH_SOURCE),
                )
            )

            r = TextDocument(
                metadata=Metadata(
                    id=pg_uri,
                    root=v.metadata.root,
                    collection=v.metadata.collection,
                ),
                document_id=page_doc_id,
                text=b"",
            )

            await flow("output").send(r)

        logger.info("PDF decoding complete")

    @staticmethod
    def add_args(parser):

        FlowProcessor.add_args(parser)

        parser.add_argument(
            "-l",
            "--languages",
            default=default_languages,
            help=f"OCR languages (default: {default_languages})",
        )


def run():

    Processor.launch(default_ident, __doc__)
