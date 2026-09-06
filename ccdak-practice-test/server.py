#!/usr/bin/env python3
"""
Simple local HTTP server for CCDAK Practice Exam web application.
Serves static files and automatically opens the browser at http://localhost:3000
"""

import http.server
import socketserver
import webbrowser
import os
import sys

PORT = 3000

class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
        super().end_headers()

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    print("=" * 60)
    print(" 🚀 CCDAK Practice Exam & Interactive Simulator Server")
    print(f" 🌐 Running at: http://localhost:{PORT}")
    print(" 📂 Serving files from:", script_dir)
    print(" Press Ctrl+C to stop the server.")
    print("=" * 60)

    try:
        # Open in default web browser
        webbrowser.open(f"http://localhost:{PORT}")
    except Exception:
        pass

    with socketserver.TCPServer(("", PORT), QuietHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped.")

if __name__ == "__main__":
    main()
