import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from mycodex.db import Store
from mycodex.jobs import JobQueue
from mycodex.tracing import job_trace, export_trace


class TracingTests(unittest.TestCase):
    def test_correlates_attempts_without_exporting_sensitive_text(self):
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp)/'db.sqlite') as store:
            project = store.add_project('demo',tmp)
            queue = JobQueue(store)
            job = queue.submit('demo','prompt',{'prompt':'private task text'})
            queue.claim('owner'); queue.start(job['id'],'owner')
            run_id = store.add_run(project['id'],kind='queued',prompt='private task text')
            store.finish_run(run_id,status='completed',output_text='private output',raw_json={
                'trace_id':f"job:{job['id']}",'token_usage':{'turn':{'totalTokens':75}}})
            queue.finish(job['id'],'owner','succeeded',result={'budget_usage':{'total_tokens':75}})
            trace = job_trace(store,job['id'])
            text = json.dumps(trace)
            self.assertNotIn('private',text)
            self.assertNotIn(tmp,text)
            spans = trace['resourceSpans'][0]['scopeSpans'][0]['spans']
            self.assertEqual(len(spans),2)
            self.assertEqual(spans[0]['traceId'],spans[1]['traceId'])
            self.assertEqual(spans[1]['parentSpanId'],spans[0]['spanId'])
            self.assertEqual(len(spans[0]['traceId']),32)

    def test_real_local_http_post_and_partial_rejection(self):
        requests = []
        class Collector(BaseHTTPRequestHandler):
            def do_POST(self):
                requests.append((self.path,self.headers['Content-Type'],json.loads(self.rfile.read(int(self.headers['Content-Length'])))))
                self.send_response(200); self.end_headers()
                self.wfile.write(b'{}' if len(requests)==1 else b'{"partialSuccess":{"rejectedSpans":"1"}}')
            def log_message(self,*args):
                pass
        server = HTTPServer(('127.0.0.1',0),Collector)
        thread = threading.Thread(target=server.serve_forever,daemon=True); thread.start()
        try:
            endpoint=f'http://127.0.0.1:{server.server_port}'
            self.assertTrue(export_trace({'resourceSpans':[]},endpoint)['accepted'])
            self.assertEqual(requests[0][:2],('/v1/traces','application/json'))
            with self.assertRaises(ValueError):
                export_trace({'resourceSpans':[]},endpoint)
            with self.assertRaises(ValueError):
                export_trace({},'https://user:secret@example.org')
        finally:
            server.shutdown(); server.server_close(); thread.join()
