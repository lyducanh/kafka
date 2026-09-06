import glob
import os
import re
import json

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'CCDAK', 'CCDAK-Exam-Questions-New'))
pattern = os.path.join(base_dir, '**', 'Questions*.md')
files = sorted(glob.glob(pattern, recursive=True))

print(f"Found {len(files)} question files in {base_dir}")

questions = []
q_counter = 0

for filepath in files:
    rel_path = os.path.relpath(filepath, base_dir)
    category = rel_path.split(os.sep)[0]
    filename = os.path.basename(filepath)

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()

    # Split by ## Question X or # Question X
    sections = re.split(r'\n(?=#{1,3}\s*Question\s*\d+)', content)

    for sec in sections:
        if not re.search(r'#{1,3}\s*Question\s*\d+', sec, re.IGNORECASE):
            continue

        q_counter += 1
        num_m = re.search(r'#{1,3}\s*Question\s*(\d+)', sec, re.IGNORECASE)
        q_num = int(num_m.group(1)) if num_m else q_counter

        # Extract details part (answer & explanation)
        det_m = re.search(r'<details>(.*)', sec, re.DOTALL | re.IGNORECASE)
        if det_m:
            question_block = sec[:det_m.start()].strip()
            details_block = det_m.group(1).strip()
        else:
            question_block = sec.strip()
            details_block = ''

        # Clean details block
        details_block = re.sub(r'^<summary>.*?</summary>', '', details_block, flags=re.DOTALL | re.IGNORECASE).strip()
        details_block = re.sub(r'</details>\s*$', '', details_block, flags=re.IGNORECASE).strip()

        # Extract Prompt and Options from question_block
        # Remove the header "## Question X:"
        clean_q_block = re.sub(r'^#{1,3}\s*Question\s*\d+[:.]?', '', question_block, flags=re.IGNORECASE).strip()

        lines = clean_q_block.split('\n')
        prompt_lines = []
        options = []
        is_parsing_options = False

        for line in lines:
            stripped = line.strip()
            # Regex for option line: 1. Text, - A. Text, A. Text, - 1. Text, A) Text, etc.
            # Avoid matching lines like "1. If a broker..." if it's part of a multi-scenario question
            opt_m = re.match(r'^(?:-\s*)?([A-Za-z0-9])[\.\)]\s+(.*)', stripped)
            
            # Check if this line looks like an instruction
            is_instruction = any(stripped.lower().startswith(x) for x in [
                'choose the correct', 'choose the', 'select the', 'select two', 'select all', 'which of the'
            ])

            if opt_m and not is_instruction and (is_parsing_options or len(stripped) > 3):
                is_parsing_options = True
                opt_id = opt_m.group(1).upper()
                opt_text = opt_m.group(2).strip()
                options.append({'id': opt_id, 'text': opt_text})
            elif is_parsing_options:
                if is_instruction or stripped == '':
                    pass
                elif options:
                    # Append continuation text to the previous option
                    options[-1]['text'] += ' ' + stripped
            else:
                prompt_lines.append(line)

        prompt_text = '\n'.join(prompt_lines).strip()

        # Determine Multi-Select
        is_multi = False
        lower_prompt = (prompt_text + '\n' + clean_q_block).lower()
        if any(x in lower_prompt for x in ['select two', 'select 2', 'select three', 'select 3', 'select all', 'choose the correct answers', 'choose all']):
            is_multi = True

        # Extract Answer from details_block
        answers = []
        
        # Pattern 1: **Answer:** A or **Answer:** 1 and 4 or Answer: A, B, C
        m_ans1 = re.search(r'\*{0,2}Answer:\*{0,2}\s*([^\n\r]+)', details_block, re.IGNORECASE)
        if m_ans1:
            raw_ans = m_ans1.group(1).strip()
            # Extract letters or digits: A, B, C, D or 1, 2, 3, 4
            found_tokens = re.findall(r'\b([A-Za-z0-9])\b', raw_ans)
            answers = [t.upper() for t in found_tokens]
        
        # Pattern 2: The correct answer(s) is/are **2. ...**
        if not answers:
            m_ans2 = re.search(r'The correct answer[s]?\s*(?:is|are)\s*[:]?\s*(.*?)(?:\.\s|\n|\Z)', details_block, re.IGNORECASE)
            if m_ans2:
                raw_ans = m_ans2.group(1).strip()
                found_tokens = re.findall(r'\b([A-Za-z0-9])[\.\)]', raw_ans)
                if not found_tokens:
                    found_tokens = re.findall(r'\b([A-Za-z0-9])\b', raw_ans)
                answers = [t.upper() for t in found_tokens if t.upper() not in ['IS', 'ARE', 'AND', 'OR', 'THE']]

        # Pattern 3: Correct answer: ...
        if not answers:
            m_ans3 = re.search(r'Correct answer[s]?\s*[:]\s*(.*?)(?:\.\s|\n|\Z)', details_block, re.IGNORECASE)
            if m_ans3:
                raw_ans = m_ans3.group(1).strip()
                found_tokens = re.findall(r'\b([A-Za-z0-9])[\.\)]', raw_ans)
                if not found_tokens:
                    found_tokens = re.findall(r'\b([A-Za-z0-9])\b', raw_ans)
                answers = [t.upper() for t in found_tokens if t.upper() not in ['IS', 'ARE', 'AND', 'OR', 'THE']]

        # Pattern 4: Standalone number at the start of details (like "4\n\n**Explanation:**")
        if not answers:
            m_ans4 = re.match(r'^\s*([0-9A-Za-z])\s*(?:\n|\Z)', details_block)
            if m_ans4:
                answers = [m_ans4.group(1).upper()]

        # Special Cases handling
        if not answers:
            # Check for Broker Questions 1 Question 4
            if "ephemeral node creation sequence" in details_block.lower():
                answers = ["A", "B", "C"]
            # Kafka-Streams Questions 2 Question 16
            elif "stateful operations are those that maintain" in details_block.lower():
                answers = ["C", "D"]
            # Schema-Registry Questions 3 Question 26
            elif "/config/<subject>" in details_block.lower():
                answers = ["A"]
            # Zookeeper Questions 2 Question 20
            elif "TopicChangeRate" in question_block:
                answers = ["D"]
                if not details_block:
                    details_block = "The `TopicChangeRate` metric tracks the overall rate at which topic-related changes (including creations, modifications, and deletions) occur in the cluster."
            # Zookeeper Questions 3 Question 30
            elif "controller.quorum.fetch.timeout.ms" in question_block:
                answers = ["B"]

        if len(answers) > 1:
            is_multi = True

        # Clean explanation
        explanation = details_block.strip()

        # Map option IDs in answers to match options in question if needed
        # (e.g. if options are 1, 2, 3 and answers has 1, or options are A, B, C)
        opt_ids = [o['id'] for o in options]
        
        # If options are A, B, C, D but answers has 1, 2, 3 -> map 1->A, 2->B etc.
        if options and all(o['id'] in ['A', 'B', 'C', 'D', 'E', 'F'] for o in options):
            mapped_ans = []
            for a in answers:
                if a.isdigit() and 1 <= int(a) <= 6:
                    mapped_ans.append(chr(ord('A') + int(a) - 1))
                else:
                    mapped_ans.append(a)
            answers = mapped_ans

        # If options are 1, 2, 3, 4 but answers has A, B, C -> map A->1, B->2 etc.
        elif options and all(o['id'].isdigit() for o in options):
            mapped_ans = []
            for a in answers:
                if a.isalpha() and 'A' <= a <= 'F':
                    mapped_ans.append(str(ord(a) - ord('A') + 1))
                else:
                    mapped_ans.append(a)
            answers = mapped_ans

        qid = f"{category.lower()}-{filename.replace('.md', '').lower()}-q{q_num}"

        questions.append({
            'id': qid,
            'category': category,
            'subcategory': filename.replace('.md', ''),
            'questionNumber': q_num,
            'question': prompt_text,
            'options': options,
            'answers': answers,
            'isMultiSelect': is_multi,
            'explanation': explanation
        })

print(f"Total parsed questions: {len(questions)}")

# Validation check
no_options = [q for q in questions if not q['options']]
no_answers = [q for q in questions if not q['answers']]

print(f"Questions without options: {len(no_options)}")
print(f"Questions without answers: {len(no_answers)}")

if no_answers:
    for q in no_answers[:5]:
        print("Missing answer for:", q['id'], q['category'])

# Write output files
out_json_path = os.path.join(os.path.dirname(__file__), 'questions.json')
with open(out_json_path, 'w', encoding='utf-8') as f:
    json.dump(questions, f, indent=2, ensure_ascii=False)

out_js_path = os.path.join(os.path.dirname(__file__), 'questions-data.js')
with open(out_js_path, 'w', encoding='utf-8') as f:
    f.write("// Confluent Certified Developer for Apache Kafka (CCDAK) Practice Questions Data\n")
    f.write("var CCDAK_QUESTIONS = ")
    json.dump(questions, f, indent=2, ensure_ascii=False)
    f.write(";\nif (typeof window !== 'undefined') { window.CCDAK_QUESTIONS = CCDAK_QUESTIONS; }\n")

print(f"Successfully generated {out_json_path} and {out_js_path}")
