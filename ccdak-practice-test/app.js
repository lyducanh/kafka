/**
 * CCDAK Practice Exam & Interactive Test Engine
 * Supporting Practice Mode, Timed Exam Simulation, Flashcard Mode, and State Persistence
 */

(function () {
  'use strict';

  // Master Questions List
  const rawQuestions = window.CCDAK_QUESTIONS || [];
  if (!rawQuestions.length) {
    console.error('CCDAK questions data not loaded.');
  }

  // Application State
  const state = {
    mode: 'practice', // 'practice' | 'exam' | 'flashcard'
    allQuestions: rawQuestions,
    filteredQuestions: [...rawQuestions],
    currentIndex: 0,
    selectedCategory: 'ALL',
    statusFilter: 'all', // 'all' | 'unanswered' | 'incorrect' | 'flagged'
    searchQuery: '',

    // User Progress (Persisted in localStorage)
    userAnswers: {}, // { [questionId]: [selectedOptionIds] }
    checkedQuestions: {}, // { [questionId]: boolean } (in practice mode)
    flaggedQuestions: {}, // { [questionId]: boolean }

    // Exam Mode State
    exam: {
      active: false,
      submitted: false,
      isReviewing: false,
      questions: [], // 60 randomly chosen questions
      answers: {},
      totalTimeSeconds: 90 * 60, // 90 minutes Confluent standard
      remainingSeconds: 90 * 60,
      timerInterval: null,
      score: 0,
      percentage: 0,
      passed: false,
      categoryStats: {}
    },

    theme: 'dark'
  };

  const STORAGE_KEY = 'ccdak_interactive_test_v1';

  // DOM Elements
  const els = {
    // Mode Buttons
    modePractice: document.getElementById('modePractice'),
    modeExam: document.getElementById('modeExam'),
    modeFlashcard: document.getElementById('modeFlashcard'),
    modeButtons: document.querySelectorAll('.mode-btn'),

    // Top Stats
    progressText: document.getElementById('progressText'),
    progressPercentage: document.getElementById('progressPercentage'),
    progressBarFill: document.getElementById('progressBarFill'),
    statCorrect: document.getElementById('statCorrect'),
    statIncorrect: document.getElementById('statIncorrect'),
    statFlagged: document.getElementById('statFlagged'),
    timerBadge: document.getElementById('timerBadge'),
    timerDisplay: document.getElementById('timerDisplay'),

    // Question Views
    questionCard: document.getElementById('questionCard'),
    questionView: document.getElementById('questionView'),
    flashcardView: document.getElementById('flashcardView'),
    catTag: document.getElementById('catTag'),
    subcatTag: document.getElementById('subcatTag'),
    qNumTag: document.getElementById('qNumTag'),
    multiBadge: document.getElementById('multiBadge'),
    flagBtn: document.getElementById('flagBtn'),
    flagText: document.getElementById('flagText'),
    questionText: document.getElementById('questionText'),
    optionsList: document.getElementById('optionsList'),
    explanationCard: document.getElementById('explanationCard'),
    explanationStatus: document.getElementById('explanationStatus'),
    correctAnswerBadge: document.getElementById('correctAnswerBadge'),
    explanationContent: document.getElementById('explanationContent'),

    // Flashcard Elements
    flashcardWrapper: document.getElementById('flashcardWrapper'),
    flashcardInner: document.getElementById('flashcardInner'),
    fcCategory: document.getElementById('fcCategory'),
    fcQuestion: document.getElementById('fcQuestion'),
    fcAnswerBadge: document.getElementById('fcAnswerBadge'),
    fcExplanation: document.getElementById('fcExplanation'),

    // Navigation & Actions
    prevBtn: document.getElementById('prevBtn'),
    nextBtn: document.getElementById('nextBtn'),
    checkBtn: document.getElementById('checkBtn'),
    flipBtn: document.getElementById('flipBtn'),

    // Search & Filters
    searchInput: document.getElementById('searchInput'),
    searchClear: document.getElementById('searchClear'),
    categorySelect: document.getElementById('categorySelect'),
    filterPills: document.querySelectorAll('.filter-pill'),
    filteredCount: document.getElementById('filteredCount'),
    questionGrid: document.getElementById('questionGrid'),
    gridProgress: document.getElementById('gridProgress'),
    examSubmitBox: document.getElementById('examSubmitBox'),
    submitExamBtn: document.getElementById('submitExamBtn'),

    // Tools
    themeToggle: document.getElementById('themeToggle'),
    resetProgressBtn: document.getElementById('resetProgressBtn'),

    // Exam Modal
    resultsModal: document.getElementById('resultsModal'),
    modalScoreCircle: document.getElementById('modalScoreCircle'),
    scorePercent: document.getElementById('scorePercent'),
    passBadge: document.getElementById('passBadge'),
    examSummaryText: document.getElementById('examSummaryText'),
    categoryBreakdown: document.getElementById('categoryBreakdown'),
    reviewExamBtn: document.getElementById('reviewExamBtn'),
    retakeExamBtn: document.getElementById('retakeExamBtn'),
    backToPracticeBtn: document.getElementById('backToPracticeBtn')
  };

  /* ==========================================================
     UTILITY: Lightweight Safe Markdown to HTML Formatter
     ========================================================== */
  function formatMarkdown(text) {
    if (!text) return '';
    let escaped = text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');

    // Fenced code blocks ```code```
    escaped = escaped.replace(/```([\s\S]*?)```/g, function (match, code) {
      return `<pre><code>${code.trim()}</code></pre>`;
    });

    // Inline code `foo`
    escaped = escaped.replace(/`([^`]+)`/g, '<code>$1</code>');

    // Bold **text**
    escaped = escaped.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');

    // Bullet lists lines starting with - or *
    const lines = escaped.split('\n');
    let inList = false;
    let html = '';

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      if (/^[-*]\s+/.test(line)) {
        if (!inList) {
          html += '<ul>';
          inList = true;
        }
        html += `<li>${line.replace(/^[-*]\s+/, '')}</li>`;
      } else {
        if (inList) {
          html += '</ul>';
          inList = false;
        }
        if (line.length > 0) {
          html += `<p>${line}</p>`;
        }
      }
    }
    if (inList) {
      html += '</ul>';
    }

    return html;
  }

  /* ==========================================================
     PERSISTENCE & LOCAL STORAGE
     ========================================================== */
  function loadSavedState() {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const parsed = JSON.parse(saved);
        state.userAnswers = parsed.userAnswers || {};
        state.checkedQuestions = parsed.checkedQuestions || {};
        state.flaggedQuestions = parsed.flaggedQuestions || {};
        state.theme = parsed.theme || 'dark';
      }
      applyTheme(state.theme);
    } catch (err) {
      console.warn('Could not load saved state:', err);
    }
  }

  function saveState() {
    try {
      localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
          userAnswers: state.userAnswers,
          checkedQuestions: state.checkedQuestions,
          flaggedQuestions: state.flaggedQuestions,
          theme: state.theme
        })
      );
    } catch (err) {
      console.warn('Could not save state:', err);
    }
  }

  function applyTheme(theme) {
    state.theme = theme;
    document.documentElement.setAttribute('data-theme', theme);
    els.themeToggle.textContent = theme === 'dark' ? '☀️' : '🌙';
  }

  /* ==========================================================
     CATEGORIES & FILTER INITIALIZATION
     ========================================================== */
  function populateCategories() {
    const categoryCounts = {};
    state.allQuestions.forEach((q) => {
      categoryCounts[q.category] = (categoryCounts[q.category] || 0) + 1;
    });

    const categories = Object.keys(categoryCounts).sort();

    // Build select dropdown
    els.categorySelect.innerHTML = `<option value="ALL">All Categories (${state.allQuestions.length})</option>`;
    categories.forEach((cat) => {
      const opt = document.createElement('option');
      opt.value = cat;
      opt.textContent = `${cat} (${categoryCounts[cat]})`;
      els.categorySelect.appendChild(opt);
    });
  }

  /* ==========================================================
     FILTERING & QUESTION SELECTION
     ========================================================== */
  function getActiveQuestionList() {
    if (state.mode === 'exam') {
      return state.exam.questions;
    }
    return state.filteredQuestions;
  }

  function getCurrentQuestion() {
    const list = getActiveQuestionList();
    if (!list || !list.length) return null;
    return list[state.currentIndex];
  }

  function isQuestionCorrect(q, answers) {
    if (!q || !answers || !answers.length) return false;
    const sortedAnswers = [...answers].sort();
    const sortedExpected = [...q.answers].sort();
    if (sortedAnswers.length !== sortedExpected.length) return false;
    return sortedAnswers.every((val, idx) => val === sortedExpected[idx]);
  }

  function applyFilters() {
    if (state.mode === 'exam') {
      // In Exam mode, we don't filter out questions, all 60 are in the exam sequence
      renderQuestion();
      renderQuestionGrid();
      updateTopStats();
      return;
    }

    const query = state.searchQuery.trim().toLowerCase();
    const cat = state.selectedCategory;
    const status = state.statusFilter;

    state.filteredQuestions = state.allQuestions.filter((q) => {
      // 1. Category Filter
      if (cat !== 'ALL' && q.category !== cat) {
        return false;
      }

      // 2. Status Filter
      const userAns = state.userAnswers[q.id] || [];
      const hasAnswer = userAns.length > 0;
      const isChecked = !!state.checkedQuestions[q.id];
      const isFlagged = !!state.flaggedQuestions[q.id];
      const isCorrect = isQuestionCorrect(q, userAns);

      if (status === 'unanswered' && hasAnswer) {
        return false;
      }
      if (status === 'incorrect') {
        if (!isChecked || isCorrect) return false;
      }
      if (status === 'flagged' && !isFlagged) {
        return false;
      }

      // 3. Search Query
      if (query) {
        const textMatch = q.question.toLowerCase().includes(query);
        const explMatch = q.explanation.toLowerCase().includes(query);
        const optMatch = q.options.some((o) =>
          o.text.toLowerCase().includes(query)
        );
        const subcatMatch = (q.subcategory || '').toLowerCase().includes(query);
        if (!textMatch && !explMatch && !optMatch && !subcatMatch) {
          return false;
        }
      }

      return true;
    });

    if (state.currentIndex >= state.filteredQuestions.length) {
      state.currentIndex = Math.max(0, state.filteredQuestions.length - 1);
    }

    els.filteredCount.textContent = `${state.filteredQuestions.length} Questions`;
    renderQuestion();
    renderQuestionGrid();
    updateTopStats();
  }

  /* ==========================================================
     STATS & PROGRESS BAR
     ========================================================== */
  function updateTopStats() {
    let totalQuestions = 0;
    let correctCount = 0;
    let incorrectCount = 0;
    let flaggedCount = 0;
    let answeredCount = 0;

    if (state.mode === 'exam') {
      totalQuestions = state.exam.questions.length;
      flaggedCount = Object.keys(state.flaggedQuestions).filter((id) =>
        state.exam.questions.some((q) => q.id === id)
      ).length;

      if (state.exam.submitted || state.exam.isReviewing) {
        state.exam.questions.forEach((q) => {
          const ans = state.exam.answers[q.id] || [];
          if (ans.length > 0) {
            if (isQuestionCorrect(q, ans)) {
              correctCount++;
            } else {
              incorrectCount++;
            }
          }
        });
        answeredCount = correctCount + incorrectCount;
      } else {
        answeredCount = Object.keys(state.exam.answers).filter(
          (id) => state.exam.answers[id] && state.exam.answers[id].length > 0
        ).length;
      }
    } else {
      totalQuestions = state.allQuestions.length;
      state.allQuestions.forEach((q) => {
        const userAns = state.userAnswers[q.id] || [];
        const isChecked = !!state.checkedQuestions[q.id];
        if (state.flaggedQuestions[q.id]) {
          flaggedCount++;
        }
        if (isChecked && userAns.length > 0) {
          answeredCount++;
          if (isQuestionCorrect(q, userAns)) {
            correctCount++;
          } else {
            incorrectCount++;
          }
        }
      });
    }

    const currentList = getActiveQuestionList();
    const currDisplayIndex = currentList.length > 0 ? state.currentIndex + 1 : 0;
    els.progressText.textContent = `Question ${currDisplayIndex} of ${currentList.length}`;

    const percent =
      totalQuestions > 0
        ? Math.round((answeredCount / totalQuestions) * 100)
        : 0;
    els.progressPercentage.textContent = `${percent}% Completed`;
    els.progressBarFill.style.width = `${percent}%`;

    els.statCorrect.textContent = correctCount;
    els.statIncorrect.textContent = incorrectCount;
    els.statFlagged.textContent = flaggedCount;

    els.gridProgress.textContent = `${currDisplayIndex} / ${currentList.length}`;
  }

  /* ==========================================================
     RENDERING QUESTION CARD
     ========================================================== */
  function renderQuestion() {
    const list = getActiveQuestionList();
    const q = getCurrentQuestion();

    if (!q || !list.length) {
      els.questionText.innerHTML = `
        <div style="text-align: center; padding: 2rem; color: var(--text-secondary);">
          <p style="font-size: 1.25rem; margin-bottom: 0.5rem;">🔍 No questions match the current filter.</p>
          <p style="font-size: 0.9rem;">Try selecting "All Categories" or resetting the status filter.</p>
        </div>
      `;
      els.optionsList.innerHTML = '';
      els.explanationCard.style.display = 'none';
      els.prevBtn.disabled = true;
      els.nextBtn.disabled = true;
      els.checkBtn.disabled = true;
      return;
    }

    // Enable/Disable Nav buttons
    els.prevBtn.disabled = state.currentIndex === 0;
    els.nextBtn.disabled = state.currentIndex === list.length - 1;

    // Meta Header
    els.catTag.textContent = q.category;
    els.subcatTag.textContent = q.subcategory || 'General';
    els.qNumTag.textContent = `Q${q.questionNumber || state.currentIndex + 1}`;

    if (q.isMultiSelect) {
      els.multiBadge.style.display = 'inline-block';
      els.multiBadge.textContent = `Select ${q.answers.length} (${q.answers.length > 1 ? 'Multiple' : 'Single'})`;
    } else {
      els.multiBadge.style.display = 'none';
    }

    // Flag status
    const isFlagged = !!state.flaggedQuestions[q.id];
    els.flagBtn.classList.toggle('flagged', isFlagged);
    els.flagText.textContent = isFlagged ? 'Flagged' : 'Flag';

    // Flashcard vs Normal View
    if (state.mode === 'flashcard') {
      renderFlashcardView(q);
      return;
    }

    els.questionView.style.display = 'block';
    els.flashcardView.style.display = 'none';
    els.checkBtn.style.display = 'inline-flex';
    els.flipBtn.style.display = 'none';

    // Question Prompt
    els.questionText.innerHTML = formatMarkdown(q.question);

    // Answer retrieval based on mode
    let userAnswers = [];
    let isChecked = false;

    if (state.mode === 'exam') {
      userAnswers = state.exam.answers[q.id] || [];
      isChecked = state.exam.submitted || state.exam.isReviewing;
      // In active exam mode, disable Check Answer button
      els.checkBtn.style.display = 'none';
    } else {
      userAnswers = state.userAnswers[q.id] || [];
      isChecked = !!state.checkedQuestions[q.id];
      els.checkBtn.style.display = 'inline-flex';
      els.checkBtn.disabled = userAnswers.length === 0;
      els.checkBtn.textContent = isChecked ? 'Re-Check' : 'Check Answer';
    }

    // Render Options List
    els.optionsList.innerHTML = '';
    const isCorrect = isQuestionCorrect(q, userAnswers);

    q.options.forEach((opt) => {
      const isSelected = userAnswers.includes(opt.id);
      const isExpected = q.answers.includes(opt.id);

      const item = document.createElement('div');
      item.className = 'option-item';
      if (isSelected) item.classList.add('selected');

      // Styling when checked or post-exam review
      if (isChecked) {
        if (isExpected) {
          item.classList.add('correct');
        } else if (isSelected && !isExpected) {
          item.classList.add('incorrect');
        }
      }

      const indicator = document.createElement('div');
      indicator.className = 'option-indicator';
      indicator.textContent = opt.id;

      const optText = document.createElement('div');
      optText.className = 'option-text';
      optText.innerHTML = formatMarkdown(opt.text);

      item.appendChild(indicator);
      item.appendChild(optText);

      // Handle Option Selection
      item.addEventListener('click', () => {
        if (state.mode === 'exam' && state.exam.submitted) {
          // Exam already submitted, lock changes
          return;
        }
        handleOptionSelect(q, opt.id);
      });

      els.optionsList.appendChild(item);
    });

    // Render Explanation Card (Visible in checked practice mode or post-exam review)
    if (isChecked) {
      els.explanationCard.style.display = 'block';
      els.explanationCard.style.borderColor = isCorrect
        ? 'var(--success)'
        : 'var(--danger)';

      if (isCorrect) {
        els.explanationStatus.innerHTML = `
          <span style="color: var(--success); font-size: 1.25rem;">✓</span>
          <span style="color: var(--success); font-weight: 700;">Correct! Well done.</span>
        `;
      } else {
        els.explanationStatus.innerHTML = `
          <span style="color: var(--danger); font-size: 1.25rem;">✗</span>
          <span style="color: var(--danger); font-weight: 700;">Incorrect. Review the concept below:</span>
        `;
      }

      els.correctAnswerBadge.textContent = `Correct Answer: ${q.answers.join(', ')}`;
      els.explanationContent.innerHTML = formatMarkdown(q.explanation);
    } else {
      els.explanationCard.style.display = 'none';
    }
  }

  /* ==========================================================
     FLASHCARD VIEW RENDERING
     ========================================================== */
  function renderFlashcardView(q) {
    els.questionView.style.display = 'none';
    els.flashcardView.style.display = 'block';
    els.checkBtn.style.display = 'none';
    els.flipBtn.style.display = 'inline-flex';

    // Reset flip
    els.flashcardInner.classList.remove('flipped');

    els.fcCategory.textContent = `${q.category} · Q${q.questionNumber || state.currentIndex + 1}`;
    els.fcQuestion.innerHTML = formatMarkdown(q.question);
    els.fcAnswerBadge.textContent = `Correct Answer: ${q.answers.join(', ')}`;
    els.fcExplanation.innerHTML = formatMarkdown(q.explanation);
  }

  function toggleFlashcardFlip() {
    els.flashcardInner.classList.toggle('flipped');
  }

  /* ==========================================================
     OPTION SELECTION HANDLER
     ========================================================== */
  function handleOptionSelect(q, optionId) {
    let currentSelected = [];

    if (state.mode === 'exam') {
      currentSelected = [...(state.exam.answers[q.id] || [])];
    } else {
      currentSelected = [...(state.userAnswers[q.id] || [])];
    }

    if (q.isMultiSelect) {
      // Toggle
      if (currentSelected.includes(optionId)) {
        currentSelected = currentSelected.filter((id) => id !== optionId);
      } else {
        currentSelected.push(optionId);
      }
    } else {
      // Single selection
      currentSelected = [optionId];
    }

    if (state.mode === 'exam') {
      state.exam.answers[q.id] = currentSelected;
    } else {
      state.userAnswers[q.id] = currentSelected;
      saveState();
    }

    renderQuestion();
    renderQuestionGrid();
    updateTopStats();
  }

  /* ==========================================================
     CHECK ANSWER (Practice Mode)
     ========================================================== */
  function checkAnswer() {
    const q = getCurrentQuestion();
    if (!q) return;

    state.checkedQuestions[q.id] = true;
    saveState();
    renderQuestion();
    renderQuestionGrid();
    updateTopStats();

    // Smooth scroll explanation into view
    els.explanationCard.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }

  /* ==========================================================
     QUESTION GRID PALETTE
     ========================================================== */
  function renderQuestionGrid() {
    const list = getActiveQuestionList();
    els.questionGrid.innerHTML = '';

    list.forEach((q, idx) => {
      const btn = document.createElement('button');
      btn.className = 'grid-btn';
      btn.textContent = idx + 1;
      btn.title = `${q.category} - Question ${idx + 1}`;

      if (idx === state.currentIndex) {
        btn.classList.add('current');
      }

      const isFlagged = !!state.flaggedQuestions[q.id];
      if (isFlagged) {
        btn.classList.add('flagged');
      }

      if (state.mode === 'exam') {
        const hasAnswer =
          state.exam.answers[q.id] && state.exam.answers[q.id].length > 0;
        if (state.exam.submitted || state.exam.isReviewing) {
          if (hasAnswer) {
            const isCorrect = isQuestionCorrect(q, state.exam.answers[q.id]);
            btn.classList.add(isCorrect ? 'correct' : 'incorrect');
          }
        } else {
          if (hasAnswer) {
            btn.classList.add('answered-exam');
          }
        }
      } else {
        // Practice mode
        const isChecked = !!state.checkedQuestions[q.id];
        const userAns = state.userAnswers[q.id] || [];
        if (isChecked && userAns.length > 0) {
          const isCorrect = isQuestionCorrect(q, userAns);
          btn.classList.add(isCorrect ? 'correct' : 'incorrect');
        } else if (userAns.length > 0) {
          btn.classList.add('answered-exam');
        }
      }

      btn.addEventListener('click', () => {
        state.currentIndex = idx;
        renderQuestion();
        renderQuestionGrid();
      });

      els.questionGrid.appendChild(btn);
    });
  }

  /* ==========================================================
     TIMED EXAM SIMULATOR LOGIC
     ========================================================== */
  function startExamMode() {
    if (state.exam.active && !state.exam.submitted) {
      if (
        !confirm(
          'An exam is currently running. Do you want to restart with a new set of 60 questions?'
        )
      ) {
        return;
      }
    }

    // Select 60 randomized questions distributed across categories
    const sampleSize = Math.min(60, state.allQuestions.length);
    const shuffled = [...state.allQuestions].sort(() => 0.5 - Math.random());
    const examQuestions = shuffled.slice(0, sampleSize);

    state.exam = {
      active: true,
      submitted: false,
      isReviewing: false,
      questions: examQuestions,
      answers: {},
      totalTimeSeconds: 90 * 60,
      remainingSeconds: 90 * 60,
      timerInterval: null,
      score: 0,
      percentage: 0,
      passed: false,
      categoryStats: {}
    };

    state.currentIndex = 0;
    els.timerBadge.style.display = 'inline-flex';
    els.examSubmitBox.style.display = 'block';

    startTimer();
    renderQuestion();
    renderQuestionGrid();
    updateTopStats();
  }

  function startTimer() {
    if (state.exam.timerInterval) {
      clearInterval(state.exam.timerInterval);
    }

    updateTimerDisplay();
    state.exam.timerInterval = setInterval(() => {
      state.exam.remainingSeconds--;
      updateTimerDisplay();

      if (state.exam.remainingSeconds <= 0) {
        clearInterval(state.exam.timerInterval);
        submitExam(true); // Auto submit on timeout
      }
    }, 1000);
  }

  function updateTimerDisplay() {
    const totalSec = Math.max(0, state.exam.remainingSeconds);
    const minutes = Math.floor(totalSec / 60);
    const seconds = totalSec % 60;
    els.timerDisplay.textContent = `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;

    if (totalSec <= 300) {
      // Last 5 minutes warning red
      els.timerBadge.style.borderColor = '#ef4444';
      els.timerBadge.style.background = '#ef444433';
    }
  }

  function submitExam(forcedTimeout = false) {
    if (!forcedTimeout) {
      const answeredCount = Object.keys(state.exam.answers).filter(
        (id) => state.exam.answers[id] && state.exam.answers[id].length > 0
      ).length;
      const unanswered = state.exam.questions.length - answeredCount;

      if (unanswered > 0) {
        const proceed = confirm(
          `You have ${unanswered} unanswered question(s). Are you sure you want to finish and submit the exam?`
        );
        if (!proceed) return;
      }
    }

    // Stop timer
    if (state.exam.timerInterval) {
      clearInterval(state.exam.timerInterval);
      state.exam.timerInterval = null;
    }

    state.exam.active = false;
    state.exam.submitted = true;

    // Calculate score & category breakdown
    let correct = 0;
    const catStats = {};

    state.exam.questions.forEach((q) => {
      const cat = q.category;
      if (!catStats[cat]) {
        catStats[cat] = { total: 0, correct: 0 };
      }
      catStats[cat].total++;

      const userAns = state.exam.answers[q.id] || [];
      if (isQuestionCorrect(q, userAns)) {
        correct++;
        catStats[cat].correct++;
      }
    });

    const total = state.exam.questions.length;
    const percentage = Math.round((correct / total) * 100);
    const passed = percentage >= 75; // CCDAK standard passing mark is 75%

    state.exam.score = correct;
    state.exam.percentage = percentage;
    state.exam.passed = passed;
    state.exam.categoryStats = catStats;

    // Show Results Modal
    displayResultsModal();
  }

  function displayResultsModal() {
    const { score, percentage, passed, categoryStats, questions } = state.exam;

    els.scorePercent.textContent = `${percentage}%`;
    els.passBadge.textContent = passed ? 'PASSED (≥ 75%)' : 'DID NOT PASS (< 75%)';
    els.passBadge.className = `pass-badge ${passed ? 'passed' : 'failed'}`;

    els.modalScoreCircle.style.borderColor = passed
      ? 'var(--success)'
      : 'var(--danger)';
    els.modalScoreCircle.style.background = passed
      ? 'var(--success-bg)'
      : 'var(--danger-bg)';

    els.examSummaryText.textContent = `You scored ${score} out of ${questions.length} questions correctly. Confluent CCDAK requires a passing score of 75%.`;

    // Render Category Breakdown
    els.categoryBreakdown.innerHTML = '';
    Object.keys(categoryStats)
      .sort()
      .forEach((cat) => {
        const item = categoryStats[cat];
        const catPercent = Math.round((item.correct / item.total) * 100);
        const row = document.createElement('div');
        row.className = 'cat-stat-row';
        row.innerHTML = `
          <span style="font-weight: 600;">${cat}</span>
          <span>
            ${item.correct}/${item.total} 
            <strong style="color: ${catPercent >= 75 ? 'var(--success)' : 'var(--danger)'};">
              (${catPercent}%)
            </strong>
          </span>
        `;
        els.categoryBreakdown.appendChild(row);
      });

    els.resultsModal.style.display = 'flex';
  }

  function enterReviewMode() {
    els.resultsModal.style.display = 'none';
    state.exam.isReviewing = true;
    state.currentIndex = 0;
    renderQuestion();
    renderQuestionGrid();
    updateTopStats();
  }

  /* ==========================================================
     MODE SWITCHING
     ========================================================== */
  function switchMode(targetMode) {
    if (state.mode === targetMode) return;

    if (state.mode === 'exam' && state.exam.active && !state.exam.submitted) {
      const exitExam = confirm(
        'You have an ongoing exam. Leaving exam mode will terminate your current session. Continue?'
      );
      if (!exitExam) return;
      if (state.exam.timerInterval) {
        clearInterval(state.exam.timerInterval);
      }
      state.exam.active = false;
    }

    state.mode = targetMode;
    state.currentIndex = 0;

    // Update active tab buttons
    els.modeButtons.forEach((btn) => {
      btn.classList.toggle('active', btn.dataset.mode === targetMode);
    });

    if (targetMode === 'exam') {
      startExamMode();
    } else {
      els.timerBadge.style.display = 'none';
      els.examSubmitBox.style.display = 'none';
      applyFilters();
    }
  }

  /* ==========================================================
     RESET USER PROGRESS
     ========================================================== */
  function resetAllProgress() {
    const confirmed = confirm(
      'Are you sure you want to reset all your answers, checked questions, and flagged bookmarks?'
    );
    if (!confirmed) return;

    state.userAnswers = {};
    state.checkedQuestions = {};
    state.flaggedQuestions = {};
    saveState();
    applyFilters();
    alert('All practice progress has been reset.');
  }

  /* ==========================================================
     EVENT LISTENERS & BINDINGS
     ========================================================== */
  function setupEventListeners() {
    // Mode Switchers
    els.modePractice.addEventListener('click', () => switchMode('practice'));
    els.modeExam.addEventListener('click', () => switchMode('exam'));
    els.modeFlashcard.addEventListener('click', () => switchMode('flashcard'));

    // Navigation
    els.prevBtn.addEventListener('click', () => {
      if (state.currentIndex > 0) {
        state.currentIndex--;
        renderQuestion();
        renderQuestionGrid();
        updateTopStats();
      }
    });

    els.nextBtn.addEventListener('click', () => {
      const list = getActiveQuestionList();
      if (state.currentIndex < list.length - 1) {
        state.currentIndex++;
        renderQuestion();
        renderQuestionGrid();
        updateTopStats();
      }
    });

    // Check Answer
    els.checkBtn.addEventListener('click', checkAnswer);

    // Flip Flashcard
    els.flipBtn.addEventListener('click', toggleFlashcardFlip);
    els.flashcardWrapper.addEventListener('click', toggleFlashcardFlip);

    // Flag Question
    els.flagBtn.addEventListener('click', () => {
      const q = getCurrentQuestion();
      if (!q) return;
      state.flaggedQuestions[q.id] = !state.flaggedQuestions[q.id];
      saveState();
      renderQuestion();
      renderQuestionGrid();
      updateTopStats();
    });

    // Category Selector
    els.categorySelect.addEventListener('change', (e) => {
      state.selectedCategory = e.target.value;
      state.currentIndex = 0;
      applyFilters();
    });

    // Status Filter Pills
    els.filterPills.forEach((pill) => {
      pill.addEventListener('click', () => {
        els.filterPills.forEach((p) => p.classList.remove('active'));
        pill.classList.add('active');
        state.statusFilter = pill.dataset.filter;
        state.currentIndex = 0;
        applyFilters();
      });
    });

    // Search Input with Debounce
    let debounceTimer;
    els.searchInput.addEventListener('input', (e) => {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => {
        state.searchQuery = e.target.value;
        els.searchClear.style.display = state.searchQuery ? 'block' : 'none';
        state.currentIndex = 0;
        applyFilters();
      }, 200);
    });

    els.searchClear.addEventListener('click', () => {
      els.searchInput.value = '';
      state.searchQuery = '';
      els.searchClear.style.display = 'none';
      state.currentIndex = 0;
      applyFilters();
    });

    // Theme Toggle
    els.themeToggle.addEventListener('click', () => {
      const newTheme = state.theme === 'dark' ? 'light' : 'dark';
      applyTheme(newTheme);
      saveState();
    });

    // Reset Progress
    els.resetProgressBtn.addEventListener('click', resetAllProgress);

    // Exam Submission & Modal
    els.submitExamBtn.addEventListener('click', () => submitExam(false));
    els.reviewExamBtn.addEventListener('click', enterReviewMode);
    els.retakeExamBtn.addEventListener('click', () => {
      els.resultsModal.style.display = 'none';
      startExamMode();
    });
    els.backToPracticeBtn.addEventListener('click', () => {
      els.resultsModal.style.display = 'none';
      switchMode('practice');
    });

    // Keyboard Shortcuts
    document.addEventListener('keydown', (e) => {
      // Don't trigger if typing in input
      if (['INPUT', 'SELECT', 'TEXTAREA'].includes(document.activeElement.tagName)) {
        return;
      }

      // Close modal on Escape
      if (e.key === 'Escape') {
        if (els.resultsModal.style.display === 'flex') {
          els.resultsModal.style.display = 'none';
        }
        return;
      }

      // Option selection by number (1 - 6)
      const numKey = parseInt(e.key, 10);
      if (!isNaN(numKey) && numKey >= 1 && numKey <= 9) {
        const q = getCurrentQuestion();
        if (q && q.options && q.options[numKey - 1]) {
          handleOptionSelect(q, q.options[numKey - 1].id);
        }
        return;
      }

      // Left / Right arrows
      if (e.key === 'ArrowLeft') {
        els.prevBtn.click();
      } else if (e.key === 'ArrowRight') {
        els.nextBtn.click();
      }

      // Spacebar
      if (e.key === ' ' || e.code === 'Space') {
        e.preventDefault();
        if (state.mode === 'flashcard') {
          toggleFlashcardFlip();
        } else if (state.mode === 'practice') {
          checkAnswer();
        }
      }

      // 'F' key for flag
      if (e.key === 'f' || e.key === 'F') {
        els.flagBtn.click();
      }
    });
  }

  /* ==========================================================
     APPLICATION INITIALIZATION
     ========================================================== */
  function init() {
    loadSavedState();
    populateCategories();
    setupEventListeners();
    applyFilters();
  }

  // Run on DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
