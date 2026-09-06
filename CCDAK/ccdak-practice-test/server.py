#!/usr/bin/env python3
"""
Simple local HTTP server for CCDAK Practice Exam web application.
Serves static files, automatically opens browser, and provides
API endpoints to persist question edits and personal notes directly
to questions.json and questions-data.js on disk.
"""

import http.server
import socketserver
import webbrowser
import os
import json
import sys

PORT = 3000

class ApiServerHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        json_path = os.path.join(script_dir, 'questions.json')
        js_path = os.path.join(script_dir, 'questions-data.js')

        if self.path == '/api/save-question':
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_len).decode('utf-8')
                data = json.loads(body)

                with open(json_path, 'r', encoding='utf-8') as f:
                    questions = json.load(f)

                q_id = data.get('id')
                found = False
                for q in questions:
                    if q.get('id') == q_id:
                        found = True
                        if 'answers' in data and data['answers']:
                            q['answers'] = data['answers']
                            q['isMultiSelect'] = len(data['answers']) > 1
                        if 'explanation' in data:
                            q['explanation'] = data['explanation']
                        if 'personalNotes' in data:
                            q['personalNotes'] = data['personalNotes']
                        q['isCustom'] = True
                        break

                if found:
                    # Save questions.json
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(questions, f, indent=2, ensure_ascii=False)

                    # Save questions-data.js
                    with open(js_path, 'w', encoding='utf-8') as f:
                        f.write("// Confluent Certified Developer for Apache Kafka (CCDAK) Practice Questions Data\n")
                        f.write("var CCDAK_QUESTIONS = ")
                        json.dump(questions, f, indent=2, ensure_ascii=False)
                        f.write(";\nif (typeof window !== 'undefined') { window.CCDAK_QUESTIONS = CCDAK_QUESTIONS; }\n")

                    resp = {'status': 'success', 'message': f'Successfully persisted question [{q_id}] to questions.json on disk.'}
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(resp).encode('utf-8'))
                    print(f" [Disk Sync] Updated question {q_id} in questions.json")
                else:
                    self.send_response(404)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({'status': 'error', 'message': f'Question {q_id} not found.'}).encode('utf-8'))

            except Exception as e:
                self.send_response(500)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'status': 'error', 'message': str(e)}).encode('utf-8'))
                print(f" [Error] Failed to save question: {e}")
            return

        elif self.path == '/api/save-all-questions':
            try:
                content_len = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_len).decode('utf-8')
                questions = json.loads(body)

                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(questions, f, indent=2, ensure_ascii=False)

                with open(js_path, 'w', encoding='utf-8') as f:
                    f.write("// Confluent Certified Developer for Apache Kafka (CCDAK) Practice Questions Data\n")
                    f.write("var CCDAK_QUESTIONS = ")
                    json.dump(questions, f, indent=2, ensure_ascii=False)
                    f.write(";\nif (typeof window !== 'undefined') { window.CCDAK_QUESTIONS = CCDAK_QUESTIONS; }\n")

                resp = {'status': 'success', 'message': f'Successfully persisted {len(questions)} questions to questions.json on disk.'}
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(resp).encode('utf-8'))
                print(f" [Disk Sync] Persisted all {len(questions)} questions to questions.json")

            except Exception as e:
                self.send_response(500)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'status': 'error', 'message': str(e)}).encode('utf-8'))
                print(f" [Error] Failed to save all questions: {e}")
            return

        super().do_POST()

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    print("=" * 65)
    print(" 🚀 CCDAK Practice Exam & Interactive Simulator Server")
    print(f" 🌐 Running at: http://localhost:{PORT}")
    print(" 📂 Serving files from:", script_dir)
    print(" 💾 Disk Persistence API: ENABLED (/api/save-question)")
    print(" Press Ctrl+C to stop the server.")
    print("=" * 65)

    try:
        # Open in default web browser
        webbrowser.open(f"http://localhost:{PORT}")
    except Exception:
        pass

    with socketserver.TCPServer(("", PORT), ApiServerHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped.")

if __name__ == "__main__":
    main()
