# CCDAK Interactive Practice Exam Website

An interactive practice test and exam simulator built from 330 real Confluent Certified Developer for Apache Kafka (CCDAK) questions located in `CCDAK/CCDAK-Exam-Questions-New`.

---

## Features

- **Practice Mode**:
  - Instant feedback on every question.
  - Option verification: highlights correct answers in emerald green and mistakes in crimson red.
  - Comprehensive explanations detailing why the correct answer is right and why other options are incorrect.
- **Update Answer & Question Editor**:
  - **Edit Question Button (`✏️ Edit Answer`)**: Click on any question to change which option(s) are accepted as correct, update the explanation, or add personal notes and mnemonics.
  - **Custom Edits Persistence**: All edits are saved automatically in `localStorage` and marked with an `✏️ Custom Answer` badge.
  - **Revert Original**: Easily revert any question back to original question bank values with one click.
  - **Export Dataset (`📥`)**: Download the complete dataset including your custom updates as JSON anytime.
- **Dynamic Answer Updating & Retries**:
  - **Try Again Button (`↺ Try Again`)**: Clears check state so you can re-attempt answering without spoiler feedback.
  - **Clear Choice Button**: Unselects current choices to start fresh.
- **Timed Exam Simulation Mode**:
  - Simulates the official CCDAK test environment with **60 randomized questions** across all categories following the official CCDAK domain weights.
  - 90-minute live countdown timer with visual color alerts.
  - Prevents premature answer revelation during the exam.
  - Comprehensive scoring modal with percentage, Pass/Fail status (Confluent standard: $\ge 75\%$), and category-by-category performance breakdown.
  - Post-exam "Review Answers" mode to learn from mistakes.
- **Flashcard Mode**:
  - 3D interactive flip card display.
  - Test recall of Kafka architectural concepts, configurations, and commands before seeing the answer.
- **Advanced Filtering & Search**:
  - Filter across all 14 official CCDAK categories (Broker, Producer, Consumer, Kafka Streams, Kafka Connect, Schema Registry, Security, KSQL, etc.).
  - Quick status filter pills: **All**, **Unanswered**, **Incorrect**, and **Flagged**.
  - Real-time search bar scanning question prompts, code snippets, choices, and explanations.
- **Question Matrix Grid**:
  - Visual 330-item grid showing question status (current, answered, correct, incorrect, flagged).
  - Jump directly to any question with one click.
- **State Persistence**:
  - All progress, checked status, flagged questions, and theme selections persist automatically in `localStorage`.
- **Light / Dark Mode**:
  - Elegant dark theme inspired by modern developer environments, with easy toggle to light theme.
- **Offline & Standalone Ready**:
  - Runs with zero npm dependencies or build steps. Works directly over `file:///` or HTTP.

---

## How to Run

### Option 1: Double-Click Launcher (Windows)
Double-click `start.bat`. It will start the local server and automatically open the application in your default web browser.

### Option 2: Python HTTP Server
Run the following command in PowerShell / Terminal:
```powershell
python server.py
```
Then visit [http://localhost:3000](http://localhost:3000).

### Option 3: Direct File Opening (No Server Required)
Simply double-click `index.html` or open it in any modern browser (Chrome, Edge, Firefox, Brave):
```
C:\Users\ducanh\Downloads\kafka\ccdak-practice-test\index.html
```

---

## Keyboard Shortcuts

| Shortcut | Action |
| :--- | :--- |
| `1` – `6` | Select Option 1 through 6 |
| `Space` | Check Answer (Practice Mode) / Flip Card (Flashcard Mode) |
| `←` | Previous Question |
| `→` | Next Question |
| `F` | Toggle Flag / Bookmark |
| `Esc` | Close Exam Results Modal |

---

## Question Distribution (330 Total Questions)

| Category | Questions |
| :--- | :--- |
| **Producer** | 38 |
| **Kafka-Connect** | 36 |
| **Consumer** | 35 |
| **Schema-Registry** | 32 |
| **Zookeeper** | 30 |
| **KSQL** | 28 |
| **Broker** | 26 |
| **Kafka-Streams** | 24 |
| **Monitoring-Metrics** | 21 |
| **Security** | 21 |
| **CLI** | 19 |
| **REST Proxy** | 10 |
| **Topic** | 6 |
| **Cluster-Administration** | 4 |
