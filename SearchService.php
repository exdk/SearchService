<?php

namespace App\Services;

use App\Models\Logs\SearchLog;
use App\Models\RoleHasCategory;
use App\Models\Wiki\Article;
use Illuminate\Support\Collection;
use App\Libs\RussianStemmer;
use Illuminate\Support\Facades\Auth;
use Illuminate\Support\Facades\Cache;

/**
 * Class SearchService
 *
 * Сервис расширенного поиска по статьям.
 *
 * Поддерживает:
 * - Исправление раскладки клавиатуры (ENG <-> RUS)
 * - Подсветку найденных терминов в заголовках и тексте
 * - Формирование сниппетов вокруг найденных слов
 * - Расчёт оценки релевантности (score) для сортировки результатов
 */
class SearchService
{
    private RussianStemmer $stemmer;
    private ?string $correctedQuery = null;

    /**
     * SearchService constructor.
     *
     * Инициализирует стеммер для русского языка.
     */
    public function __construct()
    {
        $this->stemmer = new RussianStemmer();
    }

    /**
     * Выполняет поиск статей по запросу с учётом возможной некорректной раскладки.
     *
     * Логика:
     * 1. Исправляет раскладку клавиатуры (если $ignoreFix = false) через fixKeyboardLayout.
     * 2. Формирует варианты запроса (оригинальный и исправленный).
     * 3. Выполняет поиск через Article::search() для каждого варианта.
     * 4. Исключает дубли по ID статьи.
     * 5. Выбирает наиболее часто встречающийся вариант запроса как "correctedQuery".
     * 6. Для каждой найденной статьи:
     *    - Вычисляет score через calculateScore()
     *    - Формирует подсветку и сниппеты через decorateArticle()
     * 7. Фильтрует статьи с score = -1 и сортирует результаты по score (по убыванию).
     *
     * @param string $query Поисковый запрос.
     * @param bool $ignoreFix Если true, раскладка клавиатуры не исправляется.
     * @param array|null $objectIds Массив ID категорий, внутри которых выполняется поиск
     * @param string|null $childType Тип массива $objectIds, внутри которого будет выполняться поиск
     *
     * @return Collection<int, Article> Коллекция статей с подсветкой, сниппетами и оценкой score.
     */
    public function search(string $query, bool $ignoreFix = false, ?array $objectIds = null, ?string $childType = null): Collection
    {
        $query = trim($this->cleanQuery($query));
        if ($query === '') return collect();

        // ключ кэша
        $cacheKey = $this->makeCacheKey($query, $ignoreFix, $objectIds, $childType);

        return Cache::tags(['search'])->remember($cacheKey, now()->addMinutes(10), function () use ($query, $ignoreFix, $objectIds, $childType) {

            $startTime = microtime(true);

            $fixed = $ignoreFix ? $query : $this->fixKeyboardLayout($query);
            $variants = collect([$query, $fixed])->unique();

            $foundIds = collect();
            $availableCategoriesIds = RoleHasCategory::getAvailableCategories();

            foreach ($variants as $variant) {
                $foundArticles = Article::search($variant)->take(3000)->get();
                foreach ($foundArticles ?? [] as $hit) {
                    if (!in_array($hit->category_id, $availableCategoriesIds) || $hit->is_draft) {
                        continue;
                    }
                    $id = $hit['id'] ?? null;
                    if ($id && !$foundIds->has($id)) {
                        $foundIds->put($id, $variant);
                    }
                }
            }

            // fallback — fuzzy поиск
            if ($foundIds->isEmpty()) {
                $stemmedQuery = $this->normalizeByStemmer($query);
                $raw = Article::search($stemmedQuery, function ($tnt) {
                    $tnt->fuzziness = true;
                    $tnt->fuzzy_distance = 3;
                    $tnt->fuzzy_prefix_length = 2;
                })->raw();

                foreach ($raw['ids'] ?? [] as $id) {
                    if (!$foundIds->has($id)) {
                        $foundIds->put($id, $stemmedQuery);
                    }
                }

                $this->correctedQuery = $stemmedQuery !== $query ? $stemmedQuery : null;
            }

            if ($foundIds->isEmpty()) return collect();

            // основной вариант запроса
            $variantCounts = $foundIds->countBy();
            $usedVariant = $variantCounts->sortDesc()->keys()->first();
            $this->correctedQuery = ($usedVariant !== $query) ? $usedVariant : null;
            $corrected = $this->correctedQuery ?? $query;

            // загрузка статей
            $articlesQuery = Article::whereIn('id', $foundIds->keys()->all());

            if ($objectIds && $childType) {
                if ($childType === 'category') {
                    $articlesQuery->whereIn('category_id', $objectIds);
                }

                if ($childType === 'attribute') {
                    $articlesQuery->whereRaw("FIND_IN_SET(?, attribute_id)", [$objectIds]);
                }
            }

            $articles = $articlesQuery->get()
                ->sortBy(fn ($a) => array_search($a->id, $foundIds->keys()->all()));

            $durationMs = (microtime(true) - $startTime) * 1000;
            $this->logSearch($query, $this->correctedQuery, $articles->count(), $durationMs);

            // расчёт score + подсветка
            return $articles
                ->map(function (Article $article) use ($foundIds, $corrected, $query) {
                    $variant = $foundIds->get($article->id);

                    $words = collect(explode(' ', mb_strtolower($variant)))
                        ->filter(fn($w) => mb_strlen($w) >= 2)
                        ->values();

                    $stems = $words->map(fn($w) => $this->stemmer->stem($w))
                        ->unique()
                        ->values();

                    $article->score = $this->calculateScore($article, $variant, $words, $stems);

                    return $this->decorateArticle($article, $variant, $query, $corrected);
                })
                ->filter(fn($article) => $article->score !== -1)
                ->sortByDesc('score')
                ->values();
        });
    }


    /**
     * Формирует уникальный ключ для кэширования результатов поиска.
     *
     * Ключ строится на основе:
     * - поискового запроса ($query)
     * - флага игнорирования исправления раскладки ($ignoreFix)
     * - массива категорий или атрибутов ($objectIds)
     * - типа фильтрации ($childType)
     * - доступных категорий пользователя (RoleHasCategory::getAvailableCategories())
     *
     * Используется для кэширования поиска в Redis, чтобы один и тот же запрос всегда
     * получал один и тот же ключ, даже если порядок данных изменится.
     *
     * @param string       $query      Поисковый запрос.
     * @param bool         $ignoreFix  Если true, раскладка клавиатуры не исправляется.
     * @param array|null   $objectIds  Массив ID категорий или атрибутов для фильтрации.
     * @param string|null  $childType  Тип фильтрации: 'category' или 'attribute'.
     *
     * @return string Уникальный ключ для Redis-кэша.
     */
    private function makeCacheKey(string $query, bool $ignoreFix, ?array $objectIds, ?string $childType): string
    {
        return 'search:' . md5(json_encode([
            'q' => $query,
            'ignoreFix' => $ignoreFix,
            'obj' => $objectIds,
            'type' => $childType,
            'roleCategories' => RoleHasCategory::getAvailableCategories()
        ]));
    }


    /**
     * Добавляет подсветку терминов и формирует сниппеты для статьи на основе поиска.
     *
     * - Подсветка выполняется для оригинального и/или исправленного поискового запроса.
     * - Каждое слово получает CSS-класс:
     *   - 'new-term' для текущего поиска
     *   - 'old-term' для исходного запроса (если отличается)
     * - Заголовок статьи сохраняется в $article->title_highlighted с подсветкой.
     * - Сниппеты текста сохраняются в $article->snippet_highlighted с подсветкой терминов.
     *
     * @param Article $article Статья для оформления с подсветкой.
     * @param string|null $currentSearch Текущий поисковый запрос (может быть null).
     * @param string|null $originalSearch Первичный поисковый запрос (может быть null).
     * @param string|null $correctedFallback Исправленная кодировка поискового запроса (fallback).
     *
     * @return Article Статья с добавленными свойствами title_highlighted и snippet_highlighted.
     */
    public function decorateArticle(Article $article, ?string $currentSearch = null, ?string $originalSearch = null, ?string $correctedFallback = null): Article
    {
        $search = $currentSearch ?? request('search') ?? $correctedFallback;
        $original = $originalSearch ?? request('original_search');

        $terms = collect();
        $termClasses = [];

        if ($original && $original !== $search) {
            $originalTerms = $this->extractTerms($original)->map(fn($t) => $this->stemmer->stem($t));
            foreach ($originalTerms as $t) {
                $termClasses[$t] = 'old-term';
            }
            $terms = $terms->merge($originalTerms);
        }

        if ($search) {
            $newTerms = $this->extractTerms($search)->map(fn($t) => $this->stemmer->stem($t));
            foreach ($newTerms as $t) {
                $termClasses[$t] = 'new-term';
            }
            $terms = $terms->merge($newTerms);
        }

        $terms = $terms->unique()->values();

        $article->title_highlighted = $this->highlightMultiple($article->title, $terms, $termClasses);
        $article->snippet_highlighted = $this->makeSnippetsWithHighlightMultiple($article->text, $terms, $termClasses);
        $article->snippet_highlighted = collect($article->snippet_highlighted)
            ->map(fn($s) => '<p class="my-2 text-break">' . $s . '</p>')
            ->toArray();

        return $article;
    }


    /**
     * Возвращает исправленный поисковый запрос, если он был сформирован.
     *
     * @return string|null Исправленный запрос или null, если исправлений не было.
     */
    public function getCorrectedQuery(): ?string
    {
        return $this->correctedQuery;
    }


    /**
     * Вычисляет оценку (score) релевантности статьи для заданного поискового запроса.
     *
     * Алгоритм оценки:
     * 1. Если полный поисковый запрос встречается в заголовке статьи, добавляется +400.
     * 2. Если полный поисковый запрос встречается в тексте статьи, добавляется +200.
     * 3. Если полный запрос не найден:
     *    - Для каждого слова из запроса:
     *        - Если слово встречается в заголовке, добавляется +100.
     *        - Если слово встречается в тексте, добавляется +10 за каждое вхождение, максимум +100 на слово.
     *    - Если не все слова из запроса найдены в заголовке или тексте, возвращается -1.
     * 4. Если все слова встречаются в заголовке, добавляется бонус +50.
     * 5. Если слова встречаются рядом в тексте (до 20 символов между ними), добавляется +20.
     * 6. Чем раньше встречается первое слово запроса в тексте/заголовке, тем выше дополнительный бонус до +20.
     *
     * @param Article $article Статья для оценки релевантности.
     * @param string $query Полный поисковый запрос.
     * @param Collection $words Коллекция отдельных слов из запроса.
     * @param Collection $stems Коллекция стеммов слов из запроса для морфологического поиска.
     *
     * @return int Оценка релевантности статьи.
     *             Если полный запрос не найден и найдено не все слова, возвращается -1.
     */
    private function calculateScore(Article $article, string $query, Collection $words, Collection $stems): int
    {
        $title = mb_strtolower($article->title);
        $text  = mb_strtolower($article->text);
        $queryLower = mb_strtolower($query);

        $score = 0;

        $foundInTitle = mb_strpos($title, $queryLower) !== false;
        $foundInText  = mb_strpos($text, $queryLower) !== false;

        if ($foundInTitle) {
            $score += 400;
        } elseif ($foundInText) {
            $score += 200;
        }

        if (!$foundInTitle && !$foundInText) {
            $foundWords = 0;

            foreach ($words as $i => $word) {
                $stem = $stems[$i] ?? null;

                $inTitle = str_contains($title, $word);
                $count = mb_substr_count($text, $word);

                if ($count === 0 && $stem) {
                    $pattern = '/\b' . preg_quote($stem, '/') . '\p{L}*/iu';
                    if (preg_match_all($pattern, $text, $matches)) {
                        $count = count($matches[0]);
                    }
                }

                if ($inTitle || $count > 0) {
                    $foundWords++;
                    if ($inTitle) $score += 100;
                    if ($count > 0) $score += min($count * 10, 100);
                }
            }

            if ($foundWords < $words->count()) {
                return -1;
            }

            if ($words->count() > 1 && $words->every(fn($w) => str_contains($title, $w))) {
                $score += 50;
            }

            if ($words->count() > 1) {
                $pattern = implode('.{0,20}', $words->map(fn($w) => preg_quote($w))->all());
                if (preg_match("/$pattern/iu", $text)) $score += 20;
            }

            $pos = mb_strpos($title . ' ' . $text, $words[0] ?? '');
            if ($pos !== false) {
                $score += max(0, 20 - (int)($pos / 100));
            }
        }

        return ($foundInTitle || $foundInText || $score > 0) ? $score : -1;
    }


    /**
     * Создаёт набор сниппетов из текста с подсветкой указанных терминов.
     *
     * - Текст разбивается на блоки (предложения) по знакам окончания (.?! или перевод строки).
     * - Из каждого блока выбираются те, где встречаются указанные термины.
     * - В найденных блоках выполняется подсветка терминов методом highlightMultiple().
     * - Возвращается ограниченное количество сниппетов (по умолчанию до 5).
     *
     * @param string $text Исходный текст для формирования сниппетов.
     * @param Collection $terms Коллекция терминов, которые нужно подсветить.
     * @param array $termClasses Ассоциативный массив [термин => CSS-класс] для индивидуальной подсветки терминов.
     *
     * @return array Массив HTML-сниппетов с подсвеченными терминами.
     */
    private function makeSnippetsWithHighlightMultiple(string $text, Collection $terms, array $termClasses): array
    {
        $maxSnippets = 5;
        $plainText = strip_tags($text);
        $snippets = [];

        $blocks = preg_split('/(?<=[.?!\n])/u', $plainText);

        $terms = $terms->sortByDesc(fn($t) => mb_strlen($t));

        foreach ($blocks as $block) {
            $blockLower = mb_strtolower($block);
            foreach ($terms as $term) {
                if (mb_stripos($blockLower, $term) !== false) {
                    $snippets[] = $this->highlightMultiple($block, $terms, $termClasses);
                    break;
                }
            }

            if (count($snippets) >= $maxSnippets) break;
        }

        return array_slice($snippets, 0, $maxSnippets);
    }


    /**
     * Подсвечивает несколько терминов в заданном тексте, оборачивая их в теги <mark>.
     *
     * - Термины сортируются по длине (по убыванию), чтобы избежать пересечений.
     * - Каждый термин заменяется на <mark class="...">...</mark>, где класс определяется из $termClasses.
     * - Вложенные <mark> теги автоматически очищаются.
     * - Текст предварительно экранируется для защиты от XSS.
     * - Поддерживается "приближённая" подсветка для слов с расстоянием Левенштейна ≤2 для длинных терминов.
     *
     * @param string $text Исходный текст, в котором выполняется подсветка.
     * @param Collection $terms Коллекция терминов для подсветки.
     * @param array $termClasses Ассоциативный массив [термин => CSS-класс], задающий класс для каждого термина.
     *
     * @return string Текст с HTML-подсветкой терминов.
     */
    private function highlightMultiple(string $text, Collection $terms, array $termClasses): string
    {
        if ($terms->isEmpty()) return e($text);

        $textEscaped = e($text);

        foreach ($terms as $term) {
            $stem = mb_strtolower($term);
            $stemmed = $this->stemmer->stem($stem);

            $pattern = '/(' . preg_quote($stemmed, '/') . ')[\p{L}\p{M}]*/iu';
            $textEscaped = preg_replace_callback($pattern, function ($matches) use ($term, $termClasses) {
                return '<mark class="' . e($termClasses[$term] ?? 'highlight') . '">' . $matches[0] . '</mark>';
            }, $textEscaped);

            if (mb_strlen($stem) > 4) {
                $patternFuzzy = '/\b[\p{L}\p{N}]+\b/iu';
                $textEscaped = preg_replace_callback($patternFuzzy, function ($matches) use ($stem, $term, $termClasses) {
                    $word = $matches[0];
                    $lowerWord = mb_strtolower($word);

                    if ($this->levenshteinUtf8($lowerWord, $stem) <= 2) {
                        return '<mark class="' . e($termClasses[$term] ?? 'highlight') . '">' . e($word) . '</mark>';
                    }

                    return $word;
                }, $textEscaped);
            }
        }

        return $textEscaped;
    }



    /**
     * Извлекает нормализованные термины из строки запроса.
     *
     * - Разбивает строку по любым не буквенно-цифровым символам.
     * - Приводит к нижнему регистру.
     * - Отбрасывает короткие слова (1 символ).
     *
     * @param string $input Входная строка запроса.
     *
     * @return Collection Коллекция нормализованных терминов для поиска.
     */
    private function extractTerms(string $input): Collection
    {
        $input = mb_strtolower($input);
        $parts = preg_split('/[^\p{L}\p{N}]+/u', $input);
        return collect($parts)
            ->filter(fn($t) => $t !== '' && mb_strlen($t) > 1)
            ->values();
    }


    /**
     * Исправляет раскладку клавиатуры между английской (ENG) и русской (RUS).
     *
     * - Если введён текст на английской раскладке, заменяет буквы на соответствующие русские символы.
     * - Если введён текст на русской раскладке, заменяет буквы на соответствующие английские символы.
     * - Работает с латинскими и кириллическими буквами, а также некоторыми знаками препинания.
     * - Преобразует результат в нижний регистр.
     *
     * Примеры:
     *   fixKeyboardLayout('ghbdtn') => 'привет'
     *   fixKeyboardLayout('руддщ') => 'hello'
     *
     * @param string $input Входная строка для исправления раскладки.
     *
     * @return string Строка с исправленной раскладкой клавиатуры в нижнем регистре.
     */
    private function fixKeyboardLayout(string $input): string
    {
        $map = [
            'q' => 'й', 'w' => 'ц', 'e' => 'у', 'r' => 'к', 't' => 'е', 'y' => 'н',
            'u' => 'г', 'i' => 'ш', 'o' => 'щ', 'p' => 'з', '[' => 'х', ']' => 'ъ',
            'a' => 'ф', 's' => 'ы', 'd' => 'в', 'f' => 'а', 'g' => 'п', 'h' => 'р',
            'j' => 'о', 'k' => 'л', 'l' => 'д', ';' => 'ж', '\'' => 'э', 'z' => 'я',
            'x' => 'ч', 'c' => 'с', 'v' => 'м', 'b' => 'и', 'n' => 'т', 'm' => 'ь',
            ',' => 'б', '.' => 'ю',
        ];

        $reverseMap = array_flip($map);
        $isLatin = preg_match('/[a-z]/i', $input);

        $converted = mb_strtolower($input);
        return strtr($converted, $isLatin ? $map : $reverseMap);
    }


    /**
     * Очищает поисковый запрос от стоп-слов, приводя его к удобной форме для поиска.
     *
     * - Преобразует строку в нижний регистр.
     * - Удаляет мусорные слова (стоп-слова) из запроса.
     * - Заменяет последовательности пробелов на одинарный пробел.
     * - Обрезает пробелы в начале и конце строки.
     *
     * @param string $query Исходный поисковый запрос.
     *
     * @return string Очищенный поисковый запрос, готовый для поиска.
     */
    private static function cleanQuery(string $query): string
    {
        $stopWords = [
            'а','без','более','бы','был','была','были','было','быть',
            'в','вам','вас','весь','во','вот','все','всего','всех',
            'вы','где','да','даже','для','до','его','ее','её','если','есть',
            'еще','ещё','же','за','здесь','и','из','или','им','их','к','как','ко',
            'когда','кто','ли','либо','между','меня','мне','может','мы','на','над',
            'надо','наш','не','него','нее','неё','нет','ни','них','но','ну','о','об',
            'однако','он','она','они','оно','от','перед','по','под','после','при',
            'про','с','сам','сама','сами','само','свой','себе','себя','со','так',
            'также','такой','там','те','тем','то','тогда','того','тоже','той','только',
            'том','тот','ту','ты','у','уж','уже','хотя','чего','чей','чем','через',
            'что','чтоб','чтобы','чье','чьё','чья','эта','эти','это','этот','этом','этой',
            'эту','я'
        ];

        $pattern = '/\b(' . implode('|', $stopWords) . ')\b/iu';
        $query = mb_strtolower($query);
        $query = preg_replace($pattern, ' ', $query);
        $query = preg_replace('/\s+/', ' ', $query);

        return trim($query);
    }


    /**
     * Вычисляет UTF-8 безопасное расстояние Левенштейна между двумя строками.
     *
     * - Поддерживает кириллицу, латиницу и любые UTF-8 символы.
     * - Используется для "приближённого" сравнения слов при поиске.
     *
     * @param string $s1 Первая строка.
     * @param string $s2 Вторая строка.
     *
     * @return int Расстояние Левенштейна между строками (количество операций вставки/удаления/замены).
     */
    private function levenshteinUtf8(string $s1, string $s2): int
    {
        $s1 = preg_split('//u', $s1, -1, PREG_SPLIT_NO_EMPTY);
        $s2 = preg_split('//u', $s2, -1, PREG_SPLIT_NO_EMPTY);
        $len1 = count($s1);
        $len2 = count($s2);
        $matrix = [];

        for ($i = 0; $i <= $len1; $i++) $matrix[$i][0] = $i;
        for ($j = 0; $j <= $len2; $j++) $matrix[0][$j] = $j;

        for ($i = 1; $i <= $len1; $i++) {
            for ($j = 1; $j <= $len2; $j++) {
                $cost = ($s1[$i - 1] === $s2[$j - 1]) ? 0 : 1;
                $matrix[$i][$j] = min(
                    $matrix[$i - 1][$j] + 1,
                    $matrix[$i][$j - 1] + 1,
                    $matrix[$i - 1][$j - 1] + $cost
                );
            }
        }

        return $matrix[$len1][$len2];
    }


    /**
     * Приводит поисковый запрос к базовым стеммам для морфологического поиска.
     *
     * - Разбивает строку на слова.
     * - Отбрасывает короткие слова (1 символ).
     * - Приводит слова к стеммам с использованием RussianStemmer.
     * - Убирает дубликаты и возвращает объединённую строку.
     *
     * Пример:
     *   "документы по расходам" -> "документ по расход"
     *
     * @param string $query Исходный поисковый запрос.
     *
     * @return string Строка с нормализованными стеммами для морфологического поиска.
     */
    private function normalizeByStemmer(string $query): string
    {
        $parts = preg_split('/[^\p{L}\p{N}]+/u', mb_strtolower($query));
        $stems = collect($parts)
            ->filter(fn($t) => $t !== '' && mb_strlen($t) > 1)
            ->map(fn($t) => $this->stemmer->stem($t))
            ->unique()
            ->values();

        return implode(' ', $stems->all());
    }


    /**
     * Логирует выполненный поиск в базу данных.
     *
     * Сохраняет:
     * - исходный поисковый запрос (`query`)
     * - исправленный запрос (`correctedQuery`), если был скорректирован
     * - количество найденных результатов (`results_count`)
     * - IP пользователя и User-Agent
     * - время выполнения поиска в миллисекундах (`duration_ms`)
     * - ID пользователя (`user_id`) если пользователь авторизован
     *
     * @param string $query Исходный поисковый запрос пользователя.
     * @param string|null $corrected Исправленный поисковый запрос, если было исправление.
     * @param int $count Количество найденных результатов.
     * @param float $durationMs Время выполнения поиска в миллисекундах.
     *
     * @return void
     */
    private function logSearch(string $query, ?string $corrected, int $count, float $durationMs): void
    {
        try {
            SearchLog::create([
                'user_id'        => Auth::id(),
                'query'          => $query,
                'corrected_query'=> $corrected,
                'results_count'  => $count,
                'ip'             => request()->ip(),
                'user_agent'     => request()->header('User-Agent'),
                'duration_ms'    => round($durationMs, 2),
            ]);
        } catch (\Throwable $e) {
            //
        }
    }

}
