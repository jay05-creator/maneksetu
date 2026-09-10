import csv, hashlib, json, tempfile, zipfile
from pathlib import Path
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from core.models import DocumentChunk, IngestionRun, SourceDocument
from core.services.chunking import chunk_text
from core.services.ingestion import IngestionService

class Command(BaseCommand):
    help = "Ingest controlled Markdown, text, JSON, JSONL or CSV sources."
    def add_arguments(self, parser):
        parser.add_argument("--path", required=True)
        parser.add_argument("--source-type", choices=["demo", "official", "user"], default="demo")
    def handle(self, *args, **options):
        root = Path(options["path"]).resolve()
        if not root.exists(): raise CommandError(f"Path does not exist: {root}")
        run = IngestionRun.objects.create(source_path=str(root), status="running")
        try:
            with tempfile.TemporaryDirectory(prefix="manaksetu-ingest-") as extracted:
                if root.suffix.lower() == ".zip":
                    self.extract_zip(root, Path(extracted))
                    root = Path(extracted)
                self.ingest_path(root, run, options["source_type"])
            run.status="complete"
        except Exception as exc:
            run.status="failed"; run.failed_records=[{"error":str(exc)}]; raise
        finally:
            run.finished_at=timezone.now(); run.save()
        self.stdout.write(self.style.SUCCESS(f"Ingested {run.documents_seen} documents / {run.chunks_created} chunks"))
    def extract_zip(self, path, destination):
        allowed = {".pdf", ".html", ".htm", ".md", ".txt", ".json", ".jsonl", ".csv"}
        with zipfile.ZipFile(path) as archive:
            for member in archive.infolist():
                member_path = (destination / member.filename).resolve()
                if destination.resolve() not in member_path.parents:
                    raise ValueError(f"Unsafe archive member: {member.filename}")
                if not member.is_dir() and Path(member.filename).suffix.lower() in allowed:
                    archive.extract(member, destination)
    def ingest_path(self, root, run, source_type):
        files = [root] if root.is_file() else [x for x in root.rglob("*") if x.suffix.lower() in {".pdf",".html",".htm",".md",".txt",".json",".jsonl",".csv"}]
        for path in files:
            if path.stat().st_size > 10 * 1024 * 1024: raise ValueError(f"File exceeds 10 MB: {path.name}")
            for idx, record in enumerate(self.read_records(path)):
                content = self.record_content(record)
                document_id = str(record.get("document_id") or record.get("chunk_id") or f"{path.stem}-{idx+1}")
                title = record.get("title") or record.get("product_title_canonical") or record.get("product_title_source_list") or document_id
                doc, _ = SourceDocument.objects.update_or_create(document_id=document_id, version=int(record.get("version",1)), defaults={"title":title, "standard_number":record.get("standard_number") or record.get("is_number_source_list", ""), "content":content, "source_type":record.get("source_type",source_type), "source_url":record.get("source_url", ""), "language":record.get("language","en"), "category":record.get("category") or record.get("sector", ""), "checksum":hashlib.sha256(content.encode()).hexdigest()})
                DocumentChunk.objects.filter(document=doc).delete()
                for chunk in chunk_text(content): DocumentChunk.objects.create(document=doc, chunk_index=chunk.index, section=chunk.section, page=record.get("page") or chunk.page, text=chunk.text, token_count=len(chunk.text.split()))
                run.documents_seen += 1; run.chunks_created += doc.chunks.count()
    def record_content(self, record):
        content = record.get("content") or record.get("text") or record.get("scope")
        if content:
            return str(content)
        return json.dumps(record, ensure_ascii=False, indent=2, default=str)
    def read_records(self, path):
        if path.suffix.lower() in {".pdf", ".html", ".htm"}: return IngestionService().extract(path)
        if path.suffix == ".csv": return list(csv.DictReader(path.open(encoding="utf-8")))
        if path.suffix == ".json":
            data=json.loads(path.read_text(encoding="utf-8")); return data if isinstance(data,list) else [data]
        if path.suffix == ".jsonl": return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]
        return [{"title":path.stem, "content":path.read_text(encoding="utf-8"), "source_type":"demo"}]
