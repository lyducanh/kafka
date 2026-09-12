#!/usr/bin/env python3
"""
Compiles Markdown blog posts from `posts/*.md` into:
- blogs.json
- blogs-data.js (for offline & static zero-fetch GitHub Pages support)
"""

import os
import re
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POSTS_DIR = os.path.join(BASE_DIR, 'posts')
JSON_FILE = os.path.join(BASE_DIR, 'blogs.json')
JS_FILE = os.path.join(BASE_DIR, 'blogs-data.js')

def parse_frontmatter(content):
    """Extract metadata from YAML frontmatter."""
    meta = {}
    body = content
    if content.startswith('---'):
        parts = content.split('---', 2)
        if len(parts) >= 3:
            fm_text = parts[1].strip()
            body = parts[2].strip()
            for line in fm_text.split('\n'):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if ':' in line:
                    key, val = line.split(':', 1)
                    key = key.strip()
                    val = val.strip()
                    # Parse tags array if [a, b, c]
                    if val.startswith('[') and val.endswith(']'):
                        tags = [t.strip().strip('\'"') for t in val[1:-1].split(',') if t.strip()]
                        meta[key] = tags
                    else:
                        meta[key] = val.strip('\'"')
    return meta, body

def compile_blogs():
    if not os.path.exists(POSTS_DIR):
        os.makedirs(POSTS_DIR, exist_ok=True)
        print(f"Created {POSTS_DIR}")

    files = [f for f in os.listdir(POSTS_DIR) if f.endswith('.md')]
    posts = []

    for fname in sorted(files):
        fpath = os.path.join(POSTS_DIR, fname)
        with open(fpath, 'r', encoding='utf-8') as f:
            raw = f.read()

        meta, body = parse_frontmatter(raw)
        
        post_id = meta.get('id', os.path.splitext(fname)[0])
        post = {
            'id': post_id,
            'filename': fname,
            'title': meta.get('title', post_id.replace('-', ' ').title()),
            'title_en': meta.get('title_en', meta.get('title', '')),
            'date': meta.get('date', '2026-09-12'),
            'author': meta.get('author', 'Apache Kafka Specialist'),
            'tags': meta.get('tags', ['CCDAK', 'Kafka']),
            'summary': meta.get('summary', body[:180].replace('\n', ' ') + '...'),
            'summary_en': meta.get('summary_en', meta.get('summary', '')),
            'cover_icon': meta.get('cover_icon', '📄'),
            'reading_time': meta.get('reading_time', f"{max(1, len(body.split()) // 150)} min"),
            'content': body
        }
        posts.append(post)

    # Sort posts by date descending
    posts.sort(key=lambda p: (p.get('date', ''), p.get('id', '')), reverse=True)

    print(f"Loaded {len(posts)} blog posts from {POSTS_DIR}")

    # Write blogs.json
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(posts, f, ensure_ascii=False, indent=2)
    print(f"Wrote {JSON_FILE}")

    # Write blogs-data.js
    with open(JS_FILE, 'w', encoding='utf-8') as f:
        f.write("// Apache Kafka & CCDAK Blog Posts Data\n")
        f.write("var CCDAK_BLOGS = ")
        json.dump(posts, f, ensure_ascii=False, indent=2)
        f.write(";\n")
    print(f"Wrote {JS_FILE}")

if __name__ == '__main__':
    compile_blogs()
