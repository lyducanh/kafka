#!/usr/bin/env python3
import json
import os
import re
import time
import urllib.parse
import urllib.request
import concurrent.futures

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, 'questions.json')
OUTPUT_JS_FILE = os.path.join(BASE_DIR, 'questions-data.js')
CHECKPOINT_FILE = os.path.join(BASE_DIR, 'questions_vi_checkpoint.json')

# Post-translation replacements for common Vietnamese over-translations
POST_REPLACEMENTS = [
    (re.compile(r'\bchủ đề\b', re.IGNORECASE), 'topic'),
    (re.compile(r'\bphân vùng\b', re.IGNORECASE), 'partition'),
    (re.compile(r'\bngười tiêu dùng\b', re.IGNORECASE), 'consumer'),
    (re.compile(r'\bnhà sản xuất\b', re.IGNORECASE), 'producer'),
    (re.compile(r'\bnhóm người tiêu dùng\b', re.IGNORECASE), 'consumer group'),
    (re.compile(r'\bnhật ký\b', re.IGNORECASE), 'log'),
    (re.compile(r'\bđộ lệch\b', re.IGNORECASE), 'offset'),
    (re.compile(r'\bbản sao\b', re.IGNORECASE), 'replica'),
    (re.compile(r'\bbia mộ\b', re.IGNORECASE), 'tombstone'),
    (re.compile(r'\bnén nhật ký\b', re.IGNORECASE), 'log compaction'),
    (re.compile(r'\blưu giữ nhật ký\b', re.IGNORECASE), 'log retention'),
    (re.compile(r'\btái cân bằng\b', re.IGNORECASE), 'rebalance'),
    (re.compile(r'\bchính xác một lần\b', re.IGNORECASE), 'exactly-once'),
    (re.compile(r'\bít nhất một lần\b', re.IGNORECASE), 'at-least-once'),
    (re.compile(r'\btối đa một lần\b', re.IGNORECASE), 'at-most-once'),
    (re.compile(r'\blưu trữ trạng thái\b', re.IGNORECASE), 'state store'),
    (re.compile(r'\bđăng ký giản đồ\b', re.IGNORECASE), 'Schema Registry'),
    (re.compile(r'\bgiản đồ\b', re.IGNORECASE), 'schema'),
    (re.compile(r'\bcấu trúc liên kết\b', re.IGNORECASE), 'topology'),
    (re.compile(r'\bđầu nối\b', re.IGNORECASE), 'connector'),
    (re.compile(r'\bngười môi giới\b', re.IGNORECASE), 'broker'),
]

def protect_text(text):
    """Protect code blocks, backticks, configs, and numbers."""
    placeholders = []
    
    def add_placeholder(val, prefix='TAG'):
        idx = len(placeholders)
        tag = f'____{prefix}_{idx}____'
        placeholders.append((tag, val))
        return tag

    # 1. Protect code blocks ```...```
    def repl_cb(m):
        return add_placeholder(m.group(0), 'CB')
    text = re.sub(r'```[\s\S]*?```', repl_cb, text)

    # 2. Protect inline code `...`
    def repl_ic(m):
        return add_placeholder(m.group(0), 'IC')
    text = re.sub(r'`[^`\n]+`', repl_ic, text)

    # 3. Protect config keys like log.retention.ms, acks=all
    def repl_cfg(m):
        return add_placeholder(m.group(0), 'CFG')
    text = re.sub(r'\b[a-zA-Z0-9]+(\.[a-zA-Z0-9_-]+)+\b', repl_cfg, text)

    return text, placeholders

def restore_text(text, placeholders):
    """Restore protected placeholders."""
    for tag, val in placeholders:
        # Direct match
        text = text.replace(tag, val)
        # Handle cases where translator injected spaces
        tag_pattern = re.escape(tag).replace(r'\_', r'[\s\_]*')
        text = re.sub(tag_pattern, lambda m, v=val: v, text)
    return text

def translate_raw(text):
    """Call translation API with retry and backoff."""
    if not text or not text.strip():
        return text
    url = 'https://translate.googleapis.com/translate_a/single?client=gtx&sl=en&tl=vi&dt=t&q=' + urllib.parse.quote(text)
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
    
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                return ''.join([part[0] for part in data[0] if part[0]])
        except Exception as e:
            if attempt < 4:
                time.sleep(2 * (attempt + 1))
            else:
                raise e

def translate_with_protection(text):
    if not text or not text.strip():
        return text
    protected, placeholders = protect_text(text)
    translated = translate_raw(protected)
    restored = restore_text(translated, placeholders)
    for pattern, repl in POST_REPLACEMENTS:
        restored = pattern.sub(repl, restored)
    return restored

def process_question(q):
    """Translate question text, options, and explanation."""
    q_copy = dict(q)
    
    # 1. Question text
    q_copy['question_vi'] = translate_with_protection(q.get('question', ''))
    
    # 2. Options
    new_options = []
    for opt in q.get('options', []):
        opt_copy = dict(opt)
        opt_copy['text_vi'] = translate_with_protection(opt.get('text', ''))
        new_options.append(opt_copy)
    q_copy['options'] = new_options
    
    # 3. Explanation
    q_copy['explanation_vi'] = translate_with_protection(q.get('explanation', ''))
    
    return q_copy

def main():
    print(f"Loading {INPUT_FILE}...")
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        questions = json.load(f)
    print(f"Total questions to translate: {len(questions)}")

    translated_map = {}
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                saved = json.load(f)
                for q in saved:
                    if 'question_vi' in q and q.get('question_vi'):
                        translated_map[q['id']] = q
            print(f"Loaded {len(translated_map)} already translated questions from checkpoint.")
        except Exception as e:
            print("Checkpoint error:", e)

    results = []
    to_translate = []
    for q in questions:
        if q['id'] in translated_map:
            results.append(translated_map[q['id']])
        else:
            to_translate.append(q)

    print(f"Translating {len(to_translate)} remaining questions sequentially with rate-limit protection...")
    
    for idx, q in enumerate(to_translate, 1):
        try:
            print(f"[{idx}/{len(to_translate)}] Translating {q.get('id')}...")
            res = process_question(q)
            results.append(res)
            translated_map[q['id']] = res
            time.sleep(1.0)
        except Exception as e:
            print(f"Error on question {q.get('id')}: {e}")
            results.append(q)

        if idx % 5 == 0 or idx == len(to_translate):
            with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
                json.dump(list(translated_map.values()), f, ensure_ascii=False, indent=2)

    # Sort results back to original order
    order_map = {q['id']: idx for idx, q in enumerate(questions)}
    results.sort(key=lambda q: order_map.get(q['id'], 9999))

    # Save to questions.json
    print(f"Writing updated questions to {INPUT_FILE}...")
    with open(INPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Save to questions-data.js
    print(f"Writing updated questions-data.js to {OUTPUT_JS_FILE}...")
    with open(OUTPUT_JS_FILE, 'w', encoding='utf-8') as f:
        f.write("// Confluent Certified Developer for Apache Kafka (CCDAK) Practice Questions Data\n")
        f.write("var CCDAK_QUESTIONS = ")
        json.dump(results, f, ensure_ascii=False, indent=2)
        f.write(";\n")

    print("Translation complete!")

if __name__ == '__main__':
    main()
