/**
 * CCDAK Practice Exam & Interactive Test Engine
 * Supporting Practice Mode, Timed Exam Simulation, Flashcard Mode, and State Persistence
 */

(function () {
  'use strict';

  // Master Questions List
  function getLoadedQuestions() {
    if (typeof window !== 'undefined' && window.CCDAK_QUESTIONS && window.CCDAK_QUESTIONS.length) {
      return window.CCDAK_QUESTIONS;
    }
    if (typeof CCDAK_QUESTIONS !== 'undefined' && Array.isArray(CCDAK_QUESTIONS) && CCDAK_QUESTIONS.length) {
      return CCDAK_QUESTIONS;
    }
    return [];
  }

  const rawQuestions = getLoadedQuestions();

  /* ==========================================================
     INTERNATIONALIZATION (i18n) DICTIONARY
     ========================================================== */
  const I18N = {
    en: {
      langBtn: '🇻🇳 Tiếng Việt',
      brandTitle: 'CCDAK Exam Practice',
      brandSubtitle: '330 Real Practice Questions with Explanations',
      modePractice: 'Practice Mode',
      modeExam: 'Timed Exam',
      modeFlashcard: 'Flashcards',
      searchPlaceholder: 'Search questions...',
      themeToggleTitle: 'Toggle Light/Dark Theme',
      exportBtnTitle: 'Export questions dataset (with custom updates) to JSON',
      resetBtnTitle: 'Reset All Progress',
      questionOf: (curr, total) => `Question ${curr} of ${total}`,
      percentCompleted: (p) => `${p}% Completed`,
      statCorrect: 'Correct',
      statIncorrect: 'Incorrect',
      statFlagged: 'Flagged',
      editAnswerBtn: 'Edit Answer',
      flagText: 'Flag',
      flaggedText: 'Flagged',
      selectMultipleBadge: (n) => `Select ${n} (${n > 1 ? 'Multiple' : 'Single'})`,
      customAnswerBadge: '✏️ Custom Answer',
      checkBtn: 'Check Answer',
      recheckBtn: 'Re-Check',
      retryBtn: '↺ Try Again',
      clearChoiceBtn: 'Clear Choice',
      flipBtn: 'Flip Card ↺',
      prevBtn: '← Previous',
      nextBtn: 'Next →',
      correctStatus: 'Correct! Well done.',
      incorrectStatus: 'Incorrect. Review the concept below:',
      correctAnswerLabel: (ans) => `Correct Answer: ${ans}`,
      fcHintFront: '💡 Click to flip and reveal answer & explanation',
      fcHintBack: '🔄 Click to flip back',
      fcAnswerLabel: (ans) => `Correct Answer: ${ans}`,
      sidebarCatFilterTitle: 'Categories & Filters',
      allCategoriesOption: (count) => `All Categories (${count})`,
      filterAll: 'All',
      filterUnanswered: 'Unanswered',
      filterIncorrect: 'Incorrect',
      filterFlagged: 'Flagged',
      sidebarGridTitle: 'Question Grid',
      submitExamBtn: 'Submit Exam & View Results',
      shortcutsTitle: 'Keyboard Shortcuts',
      shortcut1: 'Select Option',
      shortcut2: 'Check Answer / Flip',
      shortcut3: 'Previous / Next',
      shortcut4: 'Flag / Unflag',
      examReadyText: 'Exam Ready (60 Questions)',
      examDurationText: '90 Minutes',
      examIntroBadge: '⏱️ Certification Simulation',
      examIntroTitle: 'Confluent CCDAK Timed Exam',
      examIntroDesc: 'Simulate official Confluent CCDAK certification test conditions with randomized blueprint questions, countdown timer, and detailed score breakdown.',
      rule60Q_title: '60 Questions',
      rule60Q_desc: 'Distributed strictly following the official CCDAK domain blueprint.',
      rule90M_title: '90 Minutes',
      rule90M_desc: 'The timer will start counting down only after you trigger the button below.',
      rule75P_title: '75% Passing Mark',
      rule75P_desc: 'Achieve at least 45 out of 60 correct answers to pass the simulation.',
      ruleLock_title: 'Exam Mode',
      ruleLock_desc: 'Answers & explanations are hidden during test and unlocked upon submission.',
      startExamBtn: '🚀 Start Timed Exam Now',
      distBlueprintTitle: '📊 Exam Blueprint Distribution',
      distTotalBadge: '60 Questions Total · 14 Topics',
      domain1_name: '🧩 Clients & Core Development',
      domain2_name: '⚡ Real-Time Stream Processing',
      domain3_name: '🔄 Integration, Governance & REST',
      domain4_name: '🖥️ Cluster Architecture & Storage',
      domain5_name: '🛡️ Security, Operations & CLI',
      noMatchFilter: '🔍 No questions match the current filter.',
      noMatchHint: 'Try selecting "All Categories" or resetting the status filter.',
      resultsModalTitle: 'Confluent CCDAK Exam Results',
      passedBadge: 'PASSED (≥ 75%)',
      failedBadge: 'DID NOT PASS (< 75%)',
      finalScore: 'Final Score',
      examSummary: (score, total) => `You scored ${score} out of ${total} questions correctly. Confluent CCDAK requires a passing score of 75%.`,
      topicBreakdownTitle: 'Topic Breakdown',
      reviewExamBtn: 'Review Questions',
      retakeExamBtn: 'Retake New Exam',
      backToPracticeBtn: 'Exit to Practice Mode',
      editModalTitle: '✏️ Update Answer & Explanation',
      formLabelQuestion: 'Question Text',
      formLabelOptions: 'Select Correct Answer(s):',
      formSubLabelOptions: 'Check the option(s) that should be accepted as the correct answer:',
      formLabelExplanation: 'Explanation & Rationale:',
      formLabelNotes: 'Personal Notes / Mnemonics (Optional):',
      saveEditBtn: '💾 Save Updates',
      restoreOriginalBtn: '↺ Revert Original',
      cancelEditBtn: 'Cancel',
      confirmExitExam: 'You have an ongoing exam. Leaving exam mode will terminate your current session. Continue?',
      confirmRestartExam: 'An exam is currently running. Do you want to restart with a new set of 60 questions?',
      confirmUnansweredSubmit: (unanswered) => `You have ${unanswered} unanswered question(s). Are you sure you want to finish and submit the exam?`,
      confirmResetProgress: 'Are you sure you want to reset all your answers, checked questions, and flagged bookmarks?',
      resetSuccessAlert: 'All practice progress has been reset.',
      savedToDiskToast: 'Saved to questions.json on disk! 💾',
      savedToBrowserToast: 'Saved in browser. (Run server.py or click 📥 to export to file).',
      revertPrompt: 'Revert this question back to the original question bank answer and explanation?'
    },
    vi: {
      langBtn: '🇬🇧 English',
      brandTitle: 'Luyện Thi CCDAK',
      brandSubtitle: '330 Câu Hỏi Thực Tế Kèm Lời Giải Chi Tiết',
      modePractice: 'Luyện Tập',
      modeExam: 'Thi Thử Tính Giờ',
      modeFlashcard: 'Thẻ Ghi Nhớ',
      searchPlaceholder: 'Tìm kiếm câu hỏi...',
      themeToggleTitle: 'Chuyển đổi Giao diện Sáng/Tối',
      exportBtnTitle: 'Xuất dữ liệu câu hỏi (kèm cập nhật) ra JSON',
      resetBtnTitle: 'Đặt lại toàn bộ tiến độ',
      questionOf: (curr, total) => `Câu ${curr} / ${total}`,
      percentCompleted: (p) => `Hoàn thành ${p}%`,
      statCorrect: 'Đúng',
      statIncorrect: 'Sai',
      statFlagged: 'Đánh dấu',
      editAnswerBtn: 'Sửa đáp án',
      flagText: 'Đánh dấu',
      flaggedText: 'Đã đánh dấu',
      selectMultipleBadge: (n) => `Chọn ${n} đáp án (${n > 1 ? 'Nhiều lựa chọn' : 'Một lựa chọn'})`,
      customAnswerBadge: '✏️ Đáp án tùy chỉnh',
      checkBtn: 'Kiểm tra đáp án',
      recheckBtn: 'Kiểm tra lại',
      retryBtn: '↺ Làm lại',
      clearChoiceBtn: 'Xóa lựa chọn',
      flipBtn: 'Lật thẻ ↺',
      prevBtn: '← Câu trước',
      nextBtn: 'Tiếp theo →',
      correctStatus: 'Chính xác! Làm rất tốt. ✓',
      incorrectStatus: 'Chưa chính xác. Hãy xem giải thích bên dưới: ✗',
      correctAnswerLabel: (ans) => `Đáp án đúng: ${ans}`,
      fcHintFront: '💡 Nhấp để lật thẻ và xem đáp án & giải thích',
      fcHintBack: '🔄 Nhấp để lật lại mặt trước',
      fcAnswerLabel: (ans) => `Đáp án đúng: ${ans}`,
      sidebarCatFilterTitle: 'Danh Mục & Bộ Lọc',
      allCategoriesOption: (count) => `Tất cả danh mục (${count})`,
      filterAll: 'Tất cả',
      filterUnanswered: 'Chưa làm',
      filterIncorrect: 'Làm sai',
      filterFlagged: 'Đã đánh dấu',
      sidebarGridTitle: 'Ma Trận Câu Hỏi',
      submitExamBtn: 'Nộp Bài Thi & Xem Kết Quả',
      shortcutsTitle: 'Phím Tắt Bàn Phím',
      shortcut1: 'Chọn đáp án',
      shortcut2: 'Kiểm tra / Lật thẻ',
      shortcut3: 'Câu trước / Tiếp theo',
      shortcut4: 'Đánh dấu / Bỏ dấu',
      examReadyText: 'Đề thi sẵn sàng (60 Câu)',
      examDurationText: '90 Phút',
      examIntroBadge: '⏱️ Mô Phỏng Kỳ Thi Chứng Chỉ',
      examIntroTitle: 'Kỳ Thi Thử Confluent CCDAK Tính Giờ',
      examIntroDesc: 'Mô phỏng sát thực tế kỳ thi chứng chỉ Confluent CCDAK với 60 câu hỏi ngẫu nhiên theo ma trận blueprint, đồng hồ đếm ngược 90 phút và bảng phân tích điểm thi chi tiết.',
      rule60Q_title: '60 Câu Hỏi',
      rule60Q_desc: 'Phân bổ chính xác theo ma trận đề thi chính thức của Confluent CCDAK.',
      rule90M_title: '90 Phút Làm Bài',
      rule90M_desc: 'Đồng hồ đếm ngược 90 phút chỉ kích hoạt khi bạn bấm nút bắt đầu thi.',
      rule75P_title: 'Điểm Đạt 75%',
      rule75P_desc: 'Cần đạt tối thiểu 45/60 câu trả lời đúng để vượt qua kỳ thi thử.',
      ruleLock_title: 'Quy Chế Thi',
      ruleLock_desc: 'Đáp án và giải thích được ẩn trong lúc làm bài và sẽ mở khóa toàn bộ khi nộp bài.',
      startExamBtn: '🚀 Bắt Đầu Làm Bài Thi Ngay',
      distBlueprintTitle: '📊 Ma Trận Phân Bổ Tỷ Trọng Đề Thi',
      distTotalBadge: 'Tổng 60 câu hỏi · 14 chủ đề',
      domain1_name: '🧩 Clients & Lập Trình Cốt Lõi',
      domain2_name: '⚡ Xử Lý Dữ Liệu Luồng Thời Gian Thực',
      domain3_name: '🔄 Tích Hợp, Quản Trị Schema & REST',
      domain4_name: '🖥️ Kiến Trúc Cụm & Lưu Trữ Broker',
      domain5_name: '🛡️ Bảo Mật, Giám Sát & Công Cụ CLI',
      noMatchFilter: '🔍 Không có câu hỏi nào khớp với bộ lọc hiện tại.',
      noMatchHint: 'Hãy thử chọn "Tất cả danh mục" hoặc đặt lại bộ lọc trạng thái.',
      resultsModalTitle: 'Kết Quả Thi Thử Confluent CCDAK',
      passedBadge: 'ĐẠT (≥ 75%)',
      failedBadge: 'CHƯA ĐẠT (< 75%)',
      finalScore: 'Điểm Tổng Kết',
      examSummary: (score, total) => `Bạn đã trả lời đúng ${score} / ${total} câu hỏi. Confluent CCDAK yêu cầu điểm số tối thiểu 75% để đạt chứng chỉ.`,
      topicBreakdownTitle: 'Phân Tích Kết Quả Theo Chủ Đề',
      reviewExamBtn: 'Xem Lại Bài Thi & Lời Giải',
      retakeExamBtn: 'Thi Lại Đề Mới',
      backToPracticeBtn: 'Trở Về Chế Độ Luyện Tập',
      editModalTitle: '✏️ Cập Nhật Đáp Án & Lời Giải',
      formLabelQuestion: 'Nội Dung Câu Hỏi',
      formLabelOptions: 'Chọn Đáp Án Đúng:',
      formSubLabelOptions: 'Tích chọn các phương án được chấp nhận là đáp án đúng:',
      formLabelExplanation: 'Giải Thích & Lý Do Chọn Đáp Án:',
      formLabelNotes: 'Ghi Chú Cá Nhân / Mẹo Ghi Nhớ (Tùy chọn):',
      saveEditBtn: '💾 Lưu Thay Đổi',
      restoreOriginalBtn: '↺ Khôi Phục Gốc',
      cancelEditBtn: 'Hủy Bỏ',
      confirmExitExam: 'Bạn đang trong bài thi tính giờ. Rời khỏi chế độ thi sẽ kết thúc phiên thi hiện tại. Bạn có chắc chắn muốn thoát?',
      confirmRestartExam: 'Một bài thi đang diễn ra. Bạn có muốn bắt đầu lại với bộ 60 câu hỏi mới không?',
      confirmUnansweredSubmit: (unanswered) => `Bạn còn ${unanswered} câu hỏi chưa trả lời. Bạn có chắc chắn muốn kết thúc và nộp bài thi?`,
      confirmResetProgress: 'Bạn có chắc chắn muốn đặt lại tất cả câu trả lời, trạng thái đã kiểm tra và câu hỏi đã đánh dấu không?',
      resetSuccessAlert: 'Tất cả tiến độ luyện tập đã được đặt lại.',
      savedToDiskToast: 'Đã lưu vào file questions.json trên đĩa! 💾',
      savedToBrowserToast: 'Đã lưu trong trình duyệt. (Chạy server.py hoặc bấm 📥 để xuất file).',
      revertPrompt: 'Khôi phục câu hỏi này về đáp án và giải thích ban đầu của ngân hàng đề thi?'
    }
  };

  // Helper function to get translated text
  function t(key, ...args) {
    const langDict = I18N[state.lang] || I18N.en;
    const val = langDict[key] !== undefined ? langDict[key] : (I18N.en[key] || key);
    return typeof val === 'function' ? val(...args) : val;
  }

  // Application State
  const state = {
    lang: 'vi', // 'vi' | 'en'
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

    theme: 'dark',
    customOverrides: {}, // { [questionId]: { answers: string[], explanation: string, notes?: string, isCustom: boolean } }
    originalQuestionsMap: {} // { [questionId]: { answers: string[], explanation: string, isMultiSelect: boolean } }
  };

  const STORAGE_KEY = 'ccdak_interactive_test_v1';
  const OVERRIDES_KEY = 'ccdak_custom_overrides_v1';

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
    examIntroView: document.getElementById('examIntroView'),
    startExamBtn: document.getElementById('startExamBtn'),
    questionFooter: document.querySelector('.question-footer'),
    flashcardView: document.getElementById('flashcardView'),
    catTag: document.getElementById('catTag'),
    subcatTag: document.getElementById('subcatTag'),
    qNumTag: document.getElementById('qNumTag'),
    multiBadge: document.getElementById('multiBadge'),
    editedBadge: document.getElementById('editedBadge'),
    editQuestionBtn: document.getElementById('editQuestionBtn'),
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
    retryBtn: document.getElementById('retryBtn'),
    clearAnswerBtn: document.getElementById('clearAnswerBtn'),
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
    langToggle: document.getElementById('langToggle'),
    themeToggle: document.getElementById('themeToggle'),
    exportDatasetBtn: document.getElementById('exportDatasetBtn'),
    resetProgressBtn: document.getElementById('resetProgressBtn'),

    // Exam Modal
    resultsModal: document.getElementById('resultsModal'),
    modalTitle: document.getElementById('modalTitle'),
    modalScoreCircle: document.getElementById('modalScoreCircle'),
    scorePercent: document.getElementById('scorePercent'),
    passBadge: document.getElementById('passBadge'),
    examSummaryText: document.getElementById('examSummaryText'),
    categoryBreakdown: document.getElementById('categoryBreakdown'),
    reviewExamBtn: document.getElementById('reviewExamBtn'),
    retakeExamBtn: document.getElementById('retakeExamBtn'),
    backToPracticeBtn: document.getElementById('backToPracticeBtn'),

    // Edit Answer Modal
    editAnswerModal: document.getElementById('editAnswerModal'),
    editModalTitle: document.getElementById('editModalTitle'),
    closeEditModalBtn: document.getElementById('closeEditModalBtn'),
    editMetaTag: document.getElementById('editMetaTag'),
    editQuestionId: document.getElementById('editQuestionId'),
    editQuestionPreview: document.getElementById('editQuestionPreview'),
    editOptionsContainer: document.getElementById('editOptionsContainer'),
    editExplanationInput: document.getElementById('editExplanationInput'),
    editNotesInput: document.getElementById('editNotesInput'),
    saveEditBtn: document.getElementById('saveEditBtn'),
    restoreOriginalBtn: document.getElementById('restoreOriginalBtn'),
    cancelEditBtn: document.getElementById('cancelEditBtn')
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
      const line = lines[i];
      const trimmed = line.trim();

      if (trimmed.startsWith('- ') || trimmed.startsWith('* ')) {
        if (!inList) {
          html += '<ul>';
          inList = true;
        }
        html += `<li>${trimmed.substring(2)}</li>`;
      } else {
        if (inList) {
          html += '</ul>';
          inList = false;
        }
        if (trimmed) {
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
        state.lang = parsed.lang || 'vi';
        state.userAnswers = parsed.userAnswers || {};
        state.checkedQuestions = parsed.checkedQuestions || {};
        state.flaggedQuestions = parsed.flaggedQuestions || {};
        state.theme = parsed.theme || 'dark';
      }
      applyTheme(state.theme);
      applyLanguage(state.lang);

      // Load custom question overrides (answer/explanation edits)
      const savedOverrides = localStorage.getItem(OVERRIDES_KEY);
      if (savedOverrides) {
        state.customOverrides = JSON.parse(savedOverrides) || {};
        applyCustomOverrides();
      }
    } catch (err) {
      console.warn('Could not load saved state:', err);
    }
  }

  function applyCustomOverrides() {
    state.allQuestions.forEach((q) => {
      // Backup original if not already recorded
      if (!state.originalQuestionsMap[q.id]) {
        state.originalQuestionsMap[q.id] = {
          answers: [...q.answers],
          explanation: q.explanation,
          isMultiSelect: q.isMultiSelect
        };
      }
      const override = state.customOverrides[q.id];
      if (override) {
        q.answers = [...override.answers];
        q.explanation = override.explanation;
        q.personalNotes = override.personalNotes || override.notes || '';
        q.customNotes = q.personalNotes;
        q.isCustom = true;
        q.isMultiSelect = override.answers.length > 1;
      }
    });
  }

  function saveCustomOverrides() {
    try {
      localStorage.setItem(OVERRIDES_KEY, JSON.stringify(state.customOverrides));
    } catch (err) {
      console.warn('Could not save overrides:', err);
    }
  }

  function saveState() {
    try {
      localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
          lang: state.lang,
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

  function applyLanguage(lang) {
    state.lang = lang;
    document.documentElement.setAttribute('lang', lang);
    if (els.langToggle) {
      els.langToggle.textContent = t('langBtn');
    }

    // Update Header
    const brandTitle = document.getElementById('brandTitle');
    if (brandTitle) brandTitle.textContent = t('brandTitle');
    const brandSubtitle = document.getElementById('brandSubtitle');
    if (brandSubtitle) brandSubtitle.textContent = t('brandSubtitle');

    const modePracticeLabel = document.getElementById('modePracticeLabel');
    if (modePracticeLabel) modePracticeLabel.textContent = t('modePractice');
    const modeExamLabel = document.getElementById('modeExamLabel');
    if (modeExamLabel) modeExamLabel.textContent = t('modeExam');
    const modeFlashcardLabel = document.getElementById('modeFlashcardLabel');
    if (modeFlashcardLabel) modeFlashcardLabel.textContent = t('modeFlashcard');

    if (els.searchInput) els.searchInput.placeholder = t('searchPlaceholder');
    if (els.themeToggle) els.themeToggle.title = t('themeToggleTitle');
    if (els.exportDatasetBtn) els.exportDatasetBtn.title = t('exportBtnTitle');
    if (els.resetProgressBtn) els.resetProgressBtn.title = t('resetBtnTitle');

    // Update Top Stats Labels
    const statCorrectLabel = document.getElementById('statCorrectLabel');
    if (statCorrectLabel) statCorrectLabel.textContent = t('statCorrect');
    const statIncorrectLabel = document.getElementById('statIncorrectLabel');
    if (statIncorrectLabel) statIncorrectLabel.textContent = t('statIncorrect');
    const statFlaggedLabel = document.getElementById('statFlaggedLabel');
    if (statFlaggedLabel) statFlaggedLabel.textContent = t('statFlagged');

    // Update Question Card Actions
    const editAnswerBtnLabel = document.getElementById('editAnswerBtnLabel');
    if (editAnswerBtnLabel) editAnswerBtnLabel.textContent = t('editAnswerBtn');

    if (els.prevBtn) els.prevBtn.textContent = t('prevBtn');
    if (els.nextBtn) els.nextBtn.textContent = t('nextBtn');
    if (els.flipBtn) els.flipBtn.textContent = t('flipBtn');

    // Update Exam Intro
    const examIntroBadge = document.getElementById('examIntroBadge');
    if (examIntroBadge) examIntroBadge.textContent = t('examIntroBadge');
    const examIntroTitle = document.getElementById('examIntroTitle');
    if (examIntroTitle) examIntroTitle.textContent = t('examIntroTitle');
    const examIntroDesc = document.getElementById('examIntroDesc');
    if (examIntroDesc) examIntroDesc.textContent = t('examIntroDesc');

    const rule60Q_title = document.getElementById('rule60Q_title');
    if (rule60Q_title) rule60Q_title.textContent = t('rule60Q_title');
    const rule60Q_desc = document.getElementById('rule60Q_desc');
    if (rule60Q_desc) rule60Q_desc.textContent = t('rule60Q_desc');

    const rule90M_title = document.getElementById('rule90M_title');
    if (rule90M_title) rule90M_title.textContent = t('rule90M_title');
    const rule90M_desc = document.getElementById('rule90M_desc');
    if (rule90M_desc) rule90M_desc.textContent = t('rule90M_desc');

    const rule75P_title = document.getElementById('rule75P_title');
    if (rule75P_title) rule75P_title.textContent = t('rule75P_title');
    const rule75P_desc = document.getElementById('rule75P_desc');
    if (rule75P_desc) rule75P_desc.textContent = t('rule75P_desc');

    const ruleLock_title = document.getElementById('ruleLock_title');
    if (ruleLock_title) ruleLock_title.textContent = t('ruleLock_title');
    const ruleLock_desc = document.getElementById('ruleLock_desc');
    if (ruleLock_desc) ruleLock_desc.textContent = t('ruleLock_desc');

    const distBlueprintTitle = document.getElementById('distBlueprintTitle');
    if (distBlueprintTitle) distBlueprintTitle.textContent = t('distBlueprintTitle');
    const distTotalBadge = document.getElementById('distTotalBadge');
    if (distTotalBadge) distTotalBadge.textContent = t('distTotalBadge');

    const domain1_name = document.getElementById('domain1_name');
    if (domain1_name) domain1_name.textContent = t('domain1_name');
    const domain2_name = document.getElementById('domain2_name');
    if (domain2_name) domain2_name.textContent = t('domain2_name');
    const domain3_name = document.getElementById('domain3_name');
    if (domain3_name) domain3_name.textContent = t('domain3_name');
    const domain4_name = document.getElementById('domain4_name');
    if (domain4_name) domain4_name.textContent = t('domain4_name');
    const domain5_name = document.getElementById('domain5_name');
    if (domain5_name) domain5_name.textContent = t('domain5_name');

    if (els.startExamBtn) els.startExamBtn.textContent = t('startExamBtn');

    const fcHintFront = document.getElementById('fcHintFront');
    if (fcHintFront) fcHintFront.textContent = t('fcHintFront');
    const fcHintBack = document.getElementById('fcHintBack');
    if (fcHintBack) fcHintBack.textContent = t('fcHintBack');

    // Sidebar
    const sidebarCatFilterTitle = document.getElementById('sidebarCatFilterTitle');
    if (sidebarCatFilterTitle) sidebarCatFilterTitle.textContent = t('sidebarCatFilterTitle');

    const filterPillAll = document.getElementById('filterPillAll');
    if (filterPillAll) filterPillAll.textContent = t('filterAll');
    const filterPillUnanswered = document.getElementById('filterPillUnanswered');
    if (filterPillUnanswered) filterPillUnanswered.textContent = t('filterUnanswered');
    const filterPillIncorrect = document.getElementById('filterPillIncorrect');
    if (filterPillIncorrect) filterPillIncorrect.textContent = t('filterIncorrect');
    const filterPillFlagged = document.getElementById('filterPillFlagged');
    if (filterPillFlagged) filterPillFlagged.textContent = t('filterFlagged');

    const sidebarGridTitle = document.getElementById('sidebarGridTitle');
    if (sidebarGridTitle) sidebarGridTitle.textContent = t('sidebarGridTitle');
    if (els.submitExamBtn) els.submitExamBtn.textContent = t('submitExamBtn');

    const shortcutsTitle = document.getElementById('shortcutsTitle');
    if (shortcutsTitle) shortcutsTitle.textContent = t('shortcutsTitle');
    const shortcut1_label = document.getElementById('shortcut1_label');
    if (shortcut1_label) shortcut1_label.textContent = t('shortcut1');
    const shortcut2_label = document.getElementById('shortcut2_label');
    if (shortcut2_label) shortcut2_label.textContent = t('shortcut2');
    const shortcut3_label = document.getElementById('shortcut3_label');
    if (shortcut3_label) shortcut3_label.textContent = t('shortcut3');
    const shortcut4_label = document.getElementById('shortcut4_label');
    if (shortcut4_label) shortcut4_label.textContent = t('shortcut4');

    // Modals
    if (els.modalTitle) els.modalTitle.textContent = t('resultsModalTitle');
    const scoreLabel = document.getElementById('scoreLabel');
    if (scoreLabel) scoreLabel.textContent = t('finalScore');
    const topicBreakdownTitle = document.getElementById('topicBreakdownTitle');
    if (topicBreakdownTitle) topicBreakdownTitle.textContent = t('topicBreakdownTitle');
    if (els.reviewExamBtn) els.reviewExamBtn.textContent = t('reviewExamBtn');
    if (els.retakeExamBtn) els.retakeExamBtn.textContent = t('retakeExamBtn');
    if (els.backToPracticeBtn) els.backToPracticeBtn.textContent = t('backToPracticeBtn');

    if (els.editModalTitle) els.editModalTitle.textContent = t('editModalTitle');
    const formLabelQuestion = document.getElementById('formLabelQuestion');
    if (formLabelQuestion) formLabelQuestion.textContent = t('formLabelQuestion');
    const formLabelOptions = document.getElementById('formLabelOptions');
    if (formLabelOptions) formLabelOptions.textContent = t('formLabelOptions');
    const formSubLabelOptions = document.getElementById('formSubLabelOptions');
    if (formSubLabelOptions) formSubLabelOptions.textContent = t('formSubLabelOptions');
    const formLabelExplanation = document.getElementById('formLabelExplanation');
    if (formLabelExplanation) formLabelExplanation.textContent = t('formLabelExplanation');
    const formLabelNotes = document.getElementById('formLabelNotes');
    if (formLabelNotes) formLabelNotes.textContent = t('formLabelNotes');
    if (els.saveEditBtn) els.saveEditBtn.textContent = t('saveEditBtn');
    if (els.restoreOriginalBtn) els.restoreOriginalBtn.textContent = t('restoreOriginalBtn');
    if (els.cancelEditBtn) els.cancelEditBtn.textContent = t('cancelEditBtn');

    populateCategories();
    renderQuestion();
    updateTopStats();
    saveState();
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
    els.categorySelect.innerHTML = `<option value="ALL">${t('allCategoriesOption', state.allQuestions.length)}</option>`;
    categories.forEach((cat) => {
      const opt = document.createElement('option');
      opt.value = cat;
      opt.textContent = `${cat} (${categoryCounts[cat]})`;
      els.categorySelect.appendChild(opt);
    });
  }

  /* ==========================================================
     LANGUAGE-AWARE QUESTION ACCESSORS
     ========================================================== */
  function getQuestionPrompt(q) {
    if (!q) return '';
    return (state.lang === 'vi' && q.question_vi) ? q.question_vi : q.question;
  }

  function getOptionText(opt) {
    if (!opt) return '';
    return (state.lang === 'vi' && opt.text_vi) ? opt.text_vi : opt.text;
  }

  function getExplanationText(q) {
    if (!q) return '';
    return (state.lang === 'vi' && q.explanation_vi) ? q.explanation_vi : q.explanation;
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

      // 3. Search Query (supports both EN & VI)
      if (query) {
        const textMatch = (q.question && q.question.toLowerCase().includes(query)) ||
          (q.question_vi && q.question_vi.toLowerCase().includes(query));
        const explMatch = (q.explanation && q.explanation.toLowerCase().includes(query)) ||
          (q.explanation_vi && q.explanation_vi.toLowerCase().includes(query));
        const optMatch = q.options.some((o) =>
          (o.text && o.text.toLowerCase().includes(query)) ||
          (o.text_vi && o.text_vi.toLowerCase().includes(query))
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
      if (!state.exam.active && !state.exam.submitted) {
        els.progressText.textContent = t('examReadyText');
        els.progressPercentage.textContent = t('examDurationText');
        els.progressBarFill.style.width = '0%';
        els.statCorrect.textContent = '0';
        els.statIncorrect.textContent = '0';
        els.statFlagged.textContent = '0';
        els.gridProgress.textContent = '0 / 60';
        return;
      }

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
    els.progressText.textContent = t('questionOf', currDisplayIndex, currentList.length);

    const percent =
      totalQuestions > 0
        ? Math.round((answeredCount / totalQuestions) * 100)
        : 0;
    els.progressPercentage.textContent = t('percentCompleted', percent);
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
    // Check if in Timed Exam Intro / Standby state
    if (state.mode === 'exam' && !state.exam.active && !state.exam.submitted) {
      if (els.examIntroView) els.examIntroView.style.display = 'block';
      els.questionView.style.display = 'none';
      els.flashcardView.style.display = 'none';
      if (els.questionFooter) els.questionFooter.style.display = 'none';

      els.catTag.textContent = state.lang === 'vi' ? 'Thi Thử' : 'Timed Exam';
      els.subcatTag.textContent = 'CCDAK Simulation';
      els.qNumTag.textContent = state.lang === 'vi' ? '60 Câu Hỏi' : '60 Questions';
      els.multiBadge.style.display = 'none';
      if (els.editedBadge) els.editedBadge.style.display = 'none';
      els.flagBtn.style.display = 'none';
      els.editQuestionBtn.style.display = 'none';
      return;
    }

    if (els.examIntroView) els.examIntroView.style.display = 'none';
    if (els.questionFooter) els.questionFooter.style.display = 'flex';
    els.flagBtn.style.display = 'inline-flex';
    els.editQuestionBtn.style.display = 'inline-flex';

    const list = getActiveQuestionList();
    const q = getCurrentQuestion();

    if (!q || !list.length) {
      els.questionText.innerHTML = `
        <div style="text-align: center; padding: 2rem; color: var(--text-secondary);">
          <p style="font-size: 1.25rem; margin-bottom: 0.5rem;">${t('noMatchFilter')}</p>
          <p style="font-size: 0.9rem;">${t('noMatchHint')}</p>
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
      els.multiBadge.textContent = t('selectMultipleBadge', q.answers.length);
    } else {
      els.multiBadge.style.display = 'none';
    }

    // Flag status
    const isFlagged = !!state.flaggedQuestions[q.id];
    els.flagBtn.classList.toggle('flagged', isFlagged);
    els.flagText.textContent = isFlagged ? t('flaggedText') : t('flagText');

    // Custom Answer Badge
    if (els.editedBadge) {
      els.editedBadge.style.display = q.isCustom ? 'inline-block' : 'none';
      els.editedBadge.textContent = t('customAnswerBadge');
    }

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
    els.questionText.innerHTML = formatMarkdown(getQuestionPrompt(q));

    // Answer retrieval based on mode
    let userAnswers = [];
    let isChecked = false;

    if (state.mode === 'exam') {
      userAnswers = state.exam.answers[q.id] || [];
      isChecked = state.exam.submitted || state.exam.isReviewing;
      // In active exam mode, disable Check Answer button
      els.checkBtn.style.display = 'none';
      if (els.retryBtn) els.retryBtn.style.display = 'none';
      if (els.clearAnswerBtn) {
        els.clearAnswerBtn.style.display = (!state.exam.submitted && userAnswers.length > 0) ? 'inline-flex' : 'none';
        els.clearAnswerBtn.textContent = t('clearChoiceBtn');
      }
    } else {
      userAnswers = state.userAnswers[q.id] || [];
      isChecked = !!state.checkedQuestions[q.id];
      els.checkBtn.style.display = 'inline-flex';
      els.checkBtn.disabled = userAnswers.length === 0;
      els.checkBtn.textContent = isChecked ? t('recheckBtn') : t('checkBtn');

      if (els.retryBtn) {
        els.retryBtn.style.display = isChecked ? 'inline-flex' : 'none';
        els.retryBtn.textContent = t('retryBtn');
      }
      if (els.clearAnswerBtn) {
        els.clearAnswerBtn.style.display = userAnswers.length > 0 ? 'inline-flex' : 'none';
        els.clearAnswerBtn.textContent = t('clearChoiceBtn');
      }
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
      optText.innerHTML = formatMarkdown(getOptionText(opt));

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
          <span style="color: var(--success); font-weight: 700;">${t('correctStatus')}</span>
        `;
      } else {
        els.explanationStatus.innerHTML = `
          <span style="color: var(--danger); font-size: 1.25rem;">✗</span>
          <span style="color: var(--danger); font-weight: 700;">${t('incorrectStatus')}</span>
        `;
      }

      els.correctAnswerBadge.textContent = t('correctAnswerLabel', q.answers.join(', ')) + (q.isCustom ? ' (Custom)' : '');
      
      let explHtml = formatMarkdown(getExplanationText(q));
      if (q.customNotes) {
        explHtml += `
          <div style="margin-top: 1rem; padding: 0.75rem 1rem; background: var(--bg-secondary); border-left: 3px solid var(--warning); border-radius: 6px;">
            <div style="font-weight: 700; color: var(--warning); margin-bottom: 0.25rem;">📝 ${state.lang === 'vi' ? 'Ghi chú cá nhân:' : 'Personal Study Notes:'}</div>
            <div>${formatMarkdown(q.customNotes)}</div>
          </div>
        `;
      }
      els.explanationContent.innerHTML = explHtml;
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
    if (els.retryBtn) els.retryBtn.style.display = 'none';
    if (els.clearAnswerBtn) els.clearAnswerBtn.style.display = 'none';
    els.flipBtn.style.display = 'inline-flex';
    els.flipBtn.textContent = t('flipBtn');

    // Reset flip
    els.flashcardInner.classList.remove('flipped');

    els.fcCategory.textContent = `${q.category} · Q${q.questionNumber || state.currentIndex + 1}${q.isCustom ? ' (Custom Answer)' : ''}`;
    els.fcQuestion.innerHTML = formatMarkdown(getQuestionPrompt(q));
    els.fcAnswerBadge.textContent = t('fcAnswerLabel', q.answers.join(', '));
    
    let fcExplHtml = formatMarkdown(getExplanationText(q));
    if (q.customNotes) {
      fcExplHtml += `
        <div style="margin-top: 0.75rem; padding: 0.5rem; background: var(--bg-primary); border-left: 3px solid var(--warning); border-radius: 4px; font-size: 0.85rem;">
          <b style="color: var(--warning);">${state.lang === 'vi' ? 'Ghi chú:' : 'Note:'}</b> ${formatMarkdown(q.customNotes)}
        </div>
      `;
    }
    els.fcExplanation.innerHTML = fcExplHtml;
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
    els.questionGrid.innerHTML = '';

    if (state.mode === 'exam' && !state.exam.active && !state.exam.submitted) {
      for (let i = 1; i <= 60; i++) {
        const btn = document.createElement('button');
        btn.className = 'grid-btn';
        btn.textContent = i;
        btn.disabled = true;
        btn.title = `Question ${i} (Unlocked when exam starts)`;
        btn.style.opacity = '0.35';
        btn.style.cursor = 'not-allowed';
        els.questionGrid.appendChild(btn);
      }
      return;
    }

    const list = getActiveQuestionList();

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
     TIMED EXAM SIMULATOR LOGIC & DISTRIBUTION
     ========================================================== */
  // Confluent CCDAK Blueprint Weights for 60 Questions
  const CCDAK_EXAM_DISTRIBUTION = {
    'Producer': 9,              // ~15%
    'Consumer': 9,              // ~15%
    'Kafka-Streams': 6,         // ~10%
    'KSQL': 6,                  // ~10%
    'Kafka-Connect': 6,         // ~10%
    'Schema-Registry': 6,       // ~10%
    'Broker': 5,                // ~8.3%
    'Security': 3,              // ~5%
    'Monitoring-Metrics': 3,    // ~5%
    'Zookeeper': 2,             // ~3.3%
    'CLI': 2,                   // ~3.3%
    'REST Proxy': 1,            // ~1.7%
    'Topic': 1,                 // ~1.7%
    'Cluster-Administration': 1 // ~1.7%
  };

  function sampleExamQuestions(allQuestions) {
    const byCategory = {};
    allQuestions.forEach((q) => {
      byCategory[q.category] = byCategory[q.category] || [];
      byCategory[q.category].push(q);
    });

    let selected = [];
    Object.keys(CCDAK_EXAM_DISTRIBUTION).forEach((cat) => {
      const quota = CCDAK_EXAM_DISTRIBUTION[cat] || 0;
      const pool = byCategory[cat] || [];
      const shuffled = [...pool].sort(() => 0.5 - Math.random());
      selected = selected.concat(shuffled.slice(0, Math.min(quota, pool.length)));
    });

    // If for any reason total is under 60, fill from remaining questions
    if (selected.length < 60) {
      const selectedIds = new Set(selected.map((q) => q.id));
      const remaining = allQuestions.filter((q) => !selectedIds.has(q.id));
      const shuffledRemaining = remaining.sort(() => 0.5 - Math.random());
      selected = selected.concat(shuffledRemaining.slice(0, 60 - selected.length));
    }

    // Shuffle the final 60 questions so they are randomized in sequence
    return selected.sort(() => 0.5 - Math.random());
  }

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

    // Select 60 questions strictly distributed by CCDAK Blueprint weights
    const examQuestions = sampleExamQuestions(state.allQuestions);

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
        const proceed = confirm(t('confirmUnansweredSubmit', unanswered));
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
    els.passBadge.textContent = passed ? t('passedBadge') : t('failedBadge');
    els.passBadge.className = `pass-badge ${passed ? 'passed' : 'failed'}`;

    els.modalScoreCircle.style.borderColor = passed
      ? 'var(--success)'
      : 'var(--danger)';
    els.modalScoreCircle.style.background = passed
      ? 'var(--success-bg)'
      : 'var(--danger-bg)';

    els.examSummaryText.textContent = t('examSummary', score, questions.length);

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
      const exitExam = confirm(t('confirmExitExam'));
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
      if (state.exam.active || state.exam.submitted) {
        els.timerBadge.style.display = state.exam.active ? 'inline-flex' : 'none';
        els.examSubmitBox.style.display = state.exam.active ? 'block' : 'none';
      } else {
        els.timerBadge.style.display = 'none';
        els.examSubmitBox.style.display = 'none';
      }
      renderQuestion();
      renderQuestionGrid();
      updateTopStats();
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
    const confirmed = confirm(t('confirmResetProgress'));
    if (!confirmed) return;

    state.userAnswers = {};
    state.checkedQuestions = {};
    state.flaggedQuestions = {};
    saveState();
    applyFilters();
    alert(t('resetSuccessAlert'));
  }

  /* ==========================================================
     EXPORT DATASET WITH USER UPDATES
     ========================================================== */
  function exportDatasetJson() {
    const dataStr =
      'data:text/json;charset=utf-8,' +
      encodeURIComponent(JSON.stringify(state.allQuestions, null, 2));
    const downloadAnchor = document.createElement('a');
    downloadAnchor.setAttribute('href', dataStr);
    downloadAnchor.setAttribute('download', 'ccdak_questions_updated.json');
    document.body.appendChild(downloadAnchor);
    downloadAnchor.click();
    downloadAnchor.remove();
  }

  /* ==========================================================
     UPDATE QUESTION ANSWER & EXPLANATION (EDIT MODAL)
     ========================================================== */
  function openEditModal(q) {
    if (!q) return;

    // Save original values if not already recorded
    if (!state.originalQuestionsMap[q.id]) {
      state.originalQuestionsMap[q.id] = {
        answers: [...q.answers],
        explanation: q.explanation,
        isMultiSelect: q.isMultiSelect
      };
    }

    els.editMetaTag.textContent = `${q.category} · Q${q.questionNumber || state.currentIndex + 1}`;
    els.editQuestionId.textContent = `[ID: ${q.id}]`;
    els.editQuestionPreview.innerHTML = formatMarkdown(getQuestionPrompt(q));
    els.editExplanationInput.value = getExplanationText(q);
    els.editNotesInput.value = q.personalNotes || q.customNotes || '';

    // Render options checkboxes
    els.editOptionsContainer.innerHTML = '';
    q.options.forEach((opt) => {
      const isChecked = q.answers.includes(opt.id);
      const label = document.createElement('label');
      label.className = `edit-option-item ${isChecked ? 'checked' : ''}`;

      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.value = opt.id;
      cb.checked = isChecked;

      cb.addEventListener('change', () => {
        label.classList.toggle('checked', cb.checked);
      });

      const optBadge = document.createElement('strong');
      optBadge.style.minWidth = '24px';
      optBadge.textContent = `${opt.id}.`;

      const optText = document.createElement('span');
      optText.style.flex = '1';
      optText.style.fontSize = '0.9rem';
      optText.innerHTML = formatMarkdown(getOptionText(opt));

      label.appendChild(cb);
      label.appendChild(optBadge);
      label.appendChild(optText);

      els.editOptionsContainer.appendChild(label);
    });

    els.editAnswerModal.style.display = 'flex';
  }

  function closeEditModal() {
    if (els.editAnswerModal) {
      els.editAnswerModal.style.display = 'none';
    }
  }

  // Toast Notification Helper
  function showToast(msg) {
    let toast = document.getElementById('appToast');
    if (!toast) {
      toast = document.createElement('div');
      toast.id = 'appToast';
      toast.style.position = 'fixed';
      toast.style.bottom = '24px';
      toast.style.right = '24px';
      toast.style.backgroundColor = 'var(--bg-secondary)';
      toast.style.color = 'var(--text-primary)';
      toast.style.border = '1px solid var(--primary)';
      toast.style.padding = '0.75rem 1.25rem';
      toast.style.borderRadius = '8px';
      toast.style.boxShadow = '0 10px 25px rgba(0,0,0,0.5)';
      toast.style.zIndex = '9999';
      toast.style.fontSize = '0.9rem';
      toast.style.fontWeight = '600';
      toast.style.transition = 'opacity 0.3s ease';
      document.body.appendChild(toast);
    }
    toast.textContent = msg;
    toast.style.opacity = '1';
    toast.style.display = 'block';
    clearTimeout(toast.timer);
    toast.timer = setTimeout(() => {
      toast.style.opacity = '0';
      setTimeout(() => { toast.style.display = 'none'; }, 300);
    }, 4000);
  }

  // Persist edits directly to questions.json on disk via server.py (local environment only)
  async function persistQuestionToDisk(q) {
    // If running in purely static hosting (e.g. GitHub Pages or file protocol), skip server API request
    if (
      window.location.protocol === 'file:' ||
      (window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1')
    ) {
      return false;
    }

    const payload = {
      id: q.id,
      answers: q.answers,
      explanation: q.explanation,
      personalNotes: q.personalNotes || q.customNotes || ''
    };

    const endpoints = [
      '/api/save-question',
      'http://localhost:3000/api/save-question'
    ];

    let success = false;
    for (const url of endpoints) {
      try {
        const resp = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        if (resp.ok) {
          const res = await resp.json();
          console.log('[Disk Sync]', res.message);
          success = true;
          break;
        }
      } catch (err) {
        // Fallback to next endpoint
      }
    }
    return success;
  }

  function saveQuestionEdits() {
    const q = getCurrentQuestion();
    if (!q) return;

    const selectedAnswers = Array.from(
      els.editOptionsContainer.querySelectorAll('input:checked')
    ).map((cb) => cb.value);

    if (selectedAnswers.length === 0) {
      alert(state.lang === 'vi' ? 'Vui lòng chọn ít nhất một đáp án đúng.' : 'Please check at least one option as the correct answer.');
      return;
    }

    const newExplanation = els.editExplanationInput.value.trim() || getExplanationText(q);
    const newNotes = els.editNotesInput.value.trim();

    // Ensure original is saved
    if (!state.originalQuestionsMap[q.id]) {
      state.originalQuestionsMap[q.id] = {
        answers: [...q.answers],
        explanation: q.explanation,
        explanation_vi: q.explanation_vi,
        isMultiSelect: q.isMultiSelect
      };
    }

    // Apply updates
    q.answers = selectedAnswers;
    if (state.lang === 'vi') {
      q.explanation_vi = newExplanation;
    } else {
      q.explanation = newExplanation;
    }
    q.personalNotes = newNotes;
    q.customNotes = newNotes;
    q.isCustom = true;
    q.isMultiSelect = selectedAnswers.length > 1;

    state.customOverrides[q.id] = {
      answers: selectedAnswers,
      explanation: q.explanation,
      explanation_vi: q.explanation_vi,
      notes: newNotes,
      personalNotes: newNotes,
      isCustom: true
    };

    saveCustomOverrides();
    closeEditModal();

    renderQuestion();
    renderQuestionGrid();
    updateTopStats();

    // Sync to disk
    persistQuestionToDisk(q).then((persisted) => {
      if (persisted) {
        showToast(t('savedToDiskToast'));
      } else {
        showToast(t('savedToBrowserToast'));
      }
    });
  }

  function restoreOriginalQuestion() {
    const q = getCurrentQuestion();
    if (!q) return;

    const orig = state.originalQuestionsMap[q.id];
    if (!orig && !q.isCustom) {
      alert(state.lang === 'vi' ? 'Câu hỏi này đã ở trạng thái gốc ban đầu.' : 'This question already has its original question bank values.');
      return;
    }

    if (!confirm(t('revertPrompt'))) {
      return;
    }

    if (orig) {
      q.answers = [...orig.answers];
      q.explanation = orig.explanation;
      if (orig.explanation_vi) q.explanation_vi = orig.explanation_vi;
      q.isMultiSelect = orig.isMultiSelect;
    }
    q.personalNotes = '';
    q.customNotes = '';
    q.isCustom = false;

    delete state.customOverrides[q.id];
    saveCustomOverrides();
    closeEditModal();

    renderQuestion();
    renderQuestionGrid();
    updateTopStats();

    // Sync revert to disk
    persistQuestionToDisk(q).then((persisted) => {
      if (persisted) {
        showToast('Reverted question in questions.json on disk! ↺');
      }
    });
  }

  /* ==========================================================
     EVENT LISTENERS & BINDINGS
     ========================================================== */
  function setupEventListeners() {
    // Language Switcher
    if (els.langToggle) {
      els.langToggle.addEventListener('click', () => {
        const nextLang = state.lang === 'vi' ? 'en' : 'vi';
        applyLanguage(nextLang);
      });
    }

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

    // Retry / Try Again
    if (els.retryBtn) {
      els.retryBtn.addEventListener('click', () => {
        const q = getCurrentQuestion();
        if (!q) return;
        state.checkedQuestions[q.id] = false;
        saveState();
        renderQuestion();
        renderQuestionGrid();
        updateTopStats();
      });
    }

    // Clear Answer Choice
    if (els.clearAnswerBtn) {
      els.clearAnswerBtn.addEventListener('click', () => {
        const q = getCurrentQuestion();
        if (!q) return;
        if (state.mode === 'exam') {
          state.exam.answers[q.id] = [];
        } else {
          state.userAnswers[q.id] = [];
          state.checkedQuestions[q.id] = false;
          saveState();
        }
        renderQuestion();
        renderQuestionGrid();
        updateTopStats();
      });
    }

    // Edit Question Modal Listeners
    if (els.editQuestionBtn) {
      els.editQuestionBtn.addEventListener('click', () => {
        const q = getCurrentQuestion();
        if (!q) return;
        openEditModal(q);
      });
    }

    if (els.closeEditModalBtn) {
      els.closeEditModalBtn.addEventListener('click', closeEditModal);
    }

    if (els.cancelEditBtn) {
      els.cancelEditBtn.addEventListener('click', closeEditModal);
    }

    if (els.saveEditBtn) {
      els.saveEditBtn.addEventListener('click', saveQuestionEdits);
    }

    if (els.restoreOriginalBtn) {
      els.restoreOriginalBtn.addEventListener('click', restoreOriginalQuestion);
    }

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

    // Export Dataset with Custom Edits
    if (els.exportDatasetBtn) {
      els.exportDatasetBtn.addEventListener('click', exportDatasetJson);
    }

    // Reset Progress
    els.resetProgressBtn.addEventListener('click', resetAllProgress);

    // Exam Trigger, Submission & Modal
    if (els.startExamBtn) {
      els.startExamBtn.addEventListener('click', startExamMode);
    }
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

      // Close modals on Escape
      if (e.key === 'Escape') {
        if (els.resultsModal.style.display === 'flex') {
          els.resultsModal.style.display = 'none';
        }
        if (els.editAnswerModal && els.editAnswerModal.style.display === 'flex') {
          closeEditModal();
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
  async function init() {
    if (!state.allQuestions.length) {
      state.allQuestions = getLoadedQuestions();
      if (!state.allQuestions.length && typeof fetch === 'function') {
        try {
          const resp = await fetch('questions.json');
          if (resp.ok) {
            state.allQuestions = await resp.json();
          }
        } catch (e) {
          console.warn('Could not fetch questions.json:', e);
        }
      }
      state.filteredQuestions = [...state.allQuestions];
    }
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
