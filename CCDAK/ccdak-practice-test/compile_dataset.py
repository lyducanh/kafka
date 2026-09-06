#!/usr/bin/env python3
"""
Compiles the 330 CCDAK questions into multiple formats:
1. Master Markdown Study Guide (CCDAK_Master_Study_Guide.md)
2. CSV Spreadsheet (ccdak_questions.csv)
3. Anki Flashcards Deck (ccdak_anki_deck.txt / .tsv)
4. Grouped JSON by Category (ccdak_by_category.json)
5. Minified Full JSON (ccdak_questions_min.json)
"""

import json
import csv
import os
import re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_FILE = os.path.join(BASE_DIR, 'questions.json')

OUTPUT_DIR = os.path.join(os.path.dirname(BASE_DIR), 'CCDAK-Compiled-Dataset')
os.makedirs(OUTPUT_DIR, exist_ok=True)

with open(JSON_FILE, 'r', encoding='utf-8') as f:
    questions = json.load(f)

print(f"Loaded {len(questions)} questions from {JSON_FILE}")

# -------------------------------------------------------------
# 1. Generate Master Study Guide Markdown
# -------------------------------------------------------------
guide_path = os.path.join(OUTPUT_DIR, 'CCDAK_Master_Study_Guide.md')

# Group by category
categories = {}
for q in questions:
    cat = q['category']
    categories.setdefault(cat, []).append(q)

with open(guide_path, 'w', encoding='utf-8') as f:
    f.write("# Confluent Certified Developer for Apache Kafka (CCDAK) Master Study Guide\n\n")
    f.write(f"> **Total Questions:** {len(questions)}  \n")
    f.write(f"> **Categories:** {len(categories)}  \n")
    f.write("> **Source:** Real CCDAK Exam Question Bank with Verified Answers & In-depth Explanations\n\n")
    f.write("---\n\n")
    
    # Table of Contents
    f.write("## 📚 Table of Contents\n\n")
    for cat in sorted(categories.keys()):
        count = len(categories[cat])
        anchor = cat.lower().replace(' ', '-').replace('/', '')
        f.write(f"- [{cat} ({count} Questions)](#{anchor})\n")
    f.write("\n---\n\n")
    
    # Questions by Category
    global_idx = 1
    for cat in sorted(categories.keys()):
        cat_anchor = cat.lower().replace(' ', '-').replace('/', '')
        f.write(f"## <a id=\"{cat_anchor}\"></a>📌 {cat} ({len(categories[cat])} Questions)\n\n")
        
        for q in categories[cat]:
            f.write(f"### Question {global_idx}: {q['id']}\n\n")
            f.write(f"**Category:** `{cat}` | **Subcategory:** `{q.get('subcategory', '')}` | **Type:** `{'Multiple Choice' if q.get('isMultiSelect') else 'Single Choice'}`\n\n")
            f.write(f"**Question:**\n\n{q['question']}\n\n")
            f.write("**Options:**\n\n")
            for opt in q['options']:
                f.write(f"- **{opt['id']}.** {opt['text']}\n")
            f.write("\n")
            
            f.write(f"<details>\n<summary><b>🔍 Reveal Answer & Explanation</b></summary>\n\n")
            f.write(f"> **Correct Answer:** `{', '.join(q['answers'])}`\n>\n")
            # Format explanation quotes
            expl_lines = q['explanation'].split('\n')
            for line in expl_lines:
                f.write(f"> {line}\n")
            if q.get('personalNotes'):
                f.write(f">\n> 📝 **Personal Study Notes:**\n")
                for note_line in q['personalNotes'].split('\n'):
                    f.write(f"> *{note_line}*\n")
            f.write("\n</details>\n\n")
            f.write("---\n\n")
            global_idx += 1

print(f"Generated Markdown Study Guide: {guide_path}")

# -------------------------------------------------------------
# 2. Generate CSV Dataset (Excel & Google Sheets ready)
# -------------------------------------------------------------
csv_path = os.path.join(OUTPUT_DIR, 'ccdak_questions.csv')
with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'Index', 'ID', 'Category', 'Subcategory', 'Question Number',
        'Is Multi Select', 'Question Text', 'Option 1', 'Option 2',
        'Option 3', 'Option 4', 'Option 5', 'Option 6', 'Correct Answers',
        'Explanation', 'Personal Notes'
    ])
    
    for idx, q in enumerate(questions, 1):
        opts = [opt['text'] for opt in q['options']]
        # Pad up to 6 options
        while len(opts) < 6:
            opts.append('')
        writer.writerow([
            idx,
            q['id'],
            q['category'],
            q.get('subcategory', ''),
            q.get('questionNumber', ''),
            'Yes' if q.get('isMultiSelect') else 'No',
            q['question'],
            opts[0], opts[1], opts[2], opts[3], opts[4], opts[5],
            ', '.join(q['answers']),
            q['explanation'],
            q.get('personalNotes', '')
        ])

print(f"Generated CSV Spreadsheet: {csv_path}")

# -------------------------------------------------------------
# 3. Generate Anki Flashcards Deck (TSV format)
# -------------------------------------------------------------
anki_path = os.path.join(OUTPUT_DIR, 'ccdak_anki_deck.tsv')
with open(anki_path, 'w', encoding='utf-8') as f:
    for q in questions:
        # Convert markdown code and newlines to HTML for Anki
        q_html = q['question'].replace('\n', '<br>')
        q_html = re.sub(r'`([^`]+)`', r'<code>\1</code>', q_html)
        
        opts_html = '<br><br><b>Options:</b><br>'
        for opt in q['options']:
            opts_html += f"<b>{opt['id']}.</b> {opt['text']}<br>"
            
        front = f"<b>[{q['category']}] Question:</b><br>{q_html}{opts_html}"
        
        ans_html = f"<b>Correct Answer:</b> <code>{', '.join(q['answers'])}</code><br><br>"
        expl_html = q['explanation'].replace('\n', '<br>')
        expl_html = re.sub(r'`([^`]+)`', r'<code>\1</code>', expl_html)
        notes_html = ''
        if q.get('personalNotes'):
            notes_html = f"<br><br><b>📝 Personal Notes:</b><br><i>{q['personalNotes'].replace(chr(10), '<br>')}</i>"
        back = f"{ans_html}<b>Explanation:</b><br>{expl_html}{notes_html}"
        
        # Clean tabs and carriage returns
        front = front.replace('\t', ' ').replace('\r', '')
        back = back.replace('\t', ' ').replace('\r', '')
        tag = f"CCDAK {q['category']}"
        
        f.write(f"{front}\t{back}\t{tag}\n")

print(f"Generated Anki Deck: {anki_path}")

# -------------------------------------------------------------
# 4. Grouped by Category JSON
# -------------------------------------------------------------
by_cat_path = os.path.join(OUTPUT_DIR, 'ccdak_by_category.json')
with open(by_cat_path, 'w', encoding='utf-8') as f:
    json.dump(categories, f, indent=2, ensure_ascii=False)

print(f"Generated JSON by Category: {by_cat_path}")

# -------------------------------------------------------------
# 5. Minified Master JSON
# -------------------------------------------------------------
min_json_path = os.path.join(OUTPUT_DIR, 'ccdak_questions_min.json')
with open(min_json_path, 'w', encoding='utf-8') as f:
    json.dump(questions, f, ensure_ascii=False, separators=(',', ':'))

print(f"Generated Minified JSON: {min_json_path}")

# Also copy all compiled datasets to ccdak-practice-test directory for convenient access
import shutil
for fname in ['CCDAK_Master_Study_Guide.md', 'ccdak_questions.csv', 'ccdak_anki_deck.tsv', 'ccdak_by_category.json']:
    src = os.path.join(OUTPUT_DIR, fname)
    dst = os.path.join(BASE_DIR, fname)
    shutil.copy2(src, dst)

print("\nAll datasets compiled successfully!")
