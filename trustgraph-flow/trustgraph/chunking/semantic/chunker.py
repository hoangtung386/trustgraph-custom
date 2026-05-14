"""
Semantic chunker for text documents.
Uses langchain_experimental SemanticChunker with sentence embeddings
to split text at natural semantic boundaries rather than fixed character
or token limits.

Supports breakpoint threshold types:
  - percentile   (default): splits at percentile gaps in cosine distance
  - standard_deviation:     splits at gaps > 1 stddev from mean
  - interquartile:          splits at gaps > 1.5x IQR
  - gradient:               splits at steepest gradient changes
"""

import logging
from prometheus_client import Histogram

from ...schema import TextDocument, Chunk, Metadata, Triples
from ...base import ChunkingService, ConsumerSpec, ProducerSpec, ParameterSpec

SemanticChunker = None
HuggingFaceEmbeddings = None

from ...provenance import (
    chunk_uri as make_chunk_uri,
    derived_entity_triples,
    set_graph,
    GRAPH_SOURCE,
)

COMPONENT_NAME = "semantic-chunker"
COMPONENT_VERSION = "1.0.0"

logger = logging.getLogger(__name__)

default_ident = "chunker"

default_threshold_type = "percentile"
default_model_name = "all-MiniLM-L6-v2"


class Processor(ChunkingService):
    def __init__(self, **params):

        id = params.get("id", default_ident)
        threshold_type = params.get("breakpoint_threshold_type", default_threshold_type)
        model_name = params.get("embeddings_model", default_model_name)

        super(Processor, self).__init__(**params | {"id": id})

        self.default_threshold_type = threshold_type
        self.default_model_name = model_name

        global SemanticChunker, HuggingFaceEmbeddings
        if SemanticChunker is None:
            from langchain_experimental.text_splitter import (
                SemanticChunker as _SemanticChunker,
            )
            from langchain_huggingface import (
                HuggingFaceEmbeddings as _HuggingFaceEmbeddings,
            )

            SemanticChunker = _SemanticChunker
            HuggingFaceEmbeddings = _HuggingFaceEmbeddings

        self.embeddings = HuggingFaceEmbeddings(model_name=model_name)

        if not hasattr(__class__, "chunk_metric"):
            __class__.chunk_metric = Histogram(
                "chunk_size",
                "Chunk size",
                ["id", "flow"],
                buckets=[
                    100,
                    160,
                    250,
                    400,
                    650,
                    1000,
                    1600,
                    2500,
                    4000,
                    6400,
                    10000,
                    16000,
                ],
            )

        self.register_specification(
            ConsumerSpec(
                name="input",
                schema=TextDocument,
                handler=self.on_message,
            )
        )

        self.register_specification(
            ProducerSpec(
                name="output",
                schema=Chunk,
            )
        )

        self.register_specification(
            ProducerSpec(
                name="triples",
                schema=Triples,
            )
        )

        self.register_specification(ParameterSpec(name="breakpoint-threshold-type"))

        logger.info(
            f"Semantic chunker initialized (model: {model_name}, "
            f"threshold: {threshold_type})"
        )

    async def on_message(self, msg, consumer, flow):

        v = msg.value()
        logger.info(f"Semantic chunking document {v.metadata.id}...")

        text = await self.get_document_text(v, flow)

        effective_threshold = self.default_threshold_type
        try:
            t = flow("breakpoint-threshold-type")
            if t is not None:
                effective_threshold = t
        except Exception as e:
            logger.warning(f"Could not parse breakpoint-threshold-type parameter: {e}")

        if isinstance(effective_threshold, str):
            effective_threshold = effective_threshold

        text_splitter = SemanticChunker(
            self.embeddings,
            breakpoint_threshold_type=effective_threshold,
        )

        texts = text_splitter.create_documents([text])

        parent_doc_id = v.document_id or v.metadata.id

        char_offset = 0

        for ix, chunk in enumerate(texts):
            chunk_index = ix + 1

            logger.debug(f"Created semantic chunk of size {len(chunk.page_content)}")

            c_uri = make_chunk_uri()
            chunk_doc_id = c_uri
            parent_uri = parent_doc_id

            chunk_content = chunk.page_content.encode("utf-8")
            chunk_length = len(chunk.page_content)

            await flow.librarian.save_child_document(
                doc_id=chunk_doc_id,
                parent_id=parent_doc_id,
                content=chunk_content,
                document_type="chunk",
                title=f"Chunk {chunk_index}",
            )

            prov_triples = derived_entity_triples(
                entity_uri=c_uri,
                parent_uri=parent_uri,
                component_name=COMPONENT_NAME,
                component_version=COMPONENT_VERSION,
                label=f"Chunk {chunk_index}",
                chunk_index=chunk_index,
                char_offset=char_offset,
                char_length=chunk_length,
            )

            await flow("triples").send(
                Triples(
                    metadata=Metadata(
                        id=c_uri,
                        root=v.metadata.root,
                        collection=v.metadata.collection,
                    ),
                    triples=set_graph(prov_triples, GRAPH_SOURCE),
                )
            )

            r = Chunk(
                metadata=Metadata(
                    id=c_uri,
                    root=v.metadata.root,
                    collection=v.metadata.collection,
                ),
                chunk=chunk_content,
                document_id=chunk_doc_id,
            )

            __class__.chunk_metric.labels(id=consumer.id, flow=consumer.flow).observe(
                chunk_length
            )

            await flow("output").send(r)

            char_offset += chunk_length

        logger.debug("Semantic document chunking complete")

    @staticmethod
    def add_args(parser):

        ChunkingService.add_args(parser)

        parser.add_argument(
            "-t",
            "--breakpoint-threshold-type",
            default=default_threshold_type,
            help=f"Threshold type: percentile, standard_deviation, interquartile, gradient (default: {default_threshold_type})",
        )

        parser.add_argument(
            "-e",
            "--embeddings-model",
            default=default_model_name,
            help=f"Sentence transformer model name (default: {default_model_name})",
        )


def run():

    Processor.launch(default_ident, __doc__)
