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
- **Markdown Blog & Knowledge Base**:
  - Full-featured technical blog for deep dives into Apache Kafka & CCDAK concepts.
  - Native Markdown parsing (`.md`) with support for headings, code blocks with syntax styling, formatted tables, lists, callout alerts, and responsive reading view.
  - Category tags filter and search across all blog posts.
  - **In-Browser Markdown Importer & Live Editor (`📝 Write / Import Markdown`)**: Drag & drop or paste any custom `.md` file to preview or publish instantly in your browser session.
  - Automated build script (`build_blogs.py`) to compile Markdown files into web-ready JSON and JavaScript data bundles.
- **Bilingual Support (EN / VI)**:
  - Complete toggle for English and Vietnamese interface, question prompts, choices, explanations, and exam feedback.
- **Offline & Standalone Ready**:
  - Runs with zero npm dependencies or build steps. Works directly over `file:///` or HTTP.

---

## How to Run & Deploy

### Option 1: GitHub Pages (Online)
This repository includes a ready-to-use GitHub Actions workflow (`.github/workflows/deploy-pages.yml`) for instant zero-config deployment:
1. Push your changes to GitHub.
2. In your repository on GitHub, go to **Settings** > **Pages**.
3. Under **Build and deployment** > **Source**, select **GitHub Actions**.
4. The workflow will automatically publish the practice test to `https://<your-username>.github.io/<repo-name>/`.

### Option 2: Direct File Opening (No Server Required)
Simply open `index.html` in any web browser (Chrome, Edge, Firefox, Safari).

### Option 3: Python Local HTTP Server
Run the following command in your terminal:
```bash
python3 server.py
```
Then visit [http://localhost:3000](http://localhost:3000).

### Option 4: Double-Click Launcher (Windows)
Double-click `start.bat` to launch the local server and open your default browser.

---

## How to Publish New Blog Posts

1. Create a new `.md` file inside `CCDAK/ccdak-practice-test/posts/` (e.g. `05_kafka_consumer_rebalance.md`).
2. Add YAML frontmatter at the top of the file:
   ```markdown
   ---
   title: "Kafka Consumer Group Rebalance Protocols"
   description: "Eager vs Cooperative Sticky rebalance protocols explained."
   date: "2026-03-15"
   author: "Kafka Architect"
   category: "Consumer"
   tags: ["Consumer", "Rebalance", "Architecture"]
   readTime: "6 min read"
   ---

   # Your Markdown content here...
   ```
3. Run the compiler script:
   ```bash
   python3 CCDAK/ccdak-practice-test/build_blogs.py
   ```
4. Commit and push your changes to GitHub! The GitHub Actions workflow will automatically build and deploy the new articles.

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
