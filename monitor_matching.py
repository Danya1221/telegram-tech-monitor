"""Pure product matching shared by live monitoring and /test."""
import re
from functools import lru_cache
from unidecode import unidecode
from rapidfuzz import fuzz

MATCH_RULESET_VERSION = '2026-10-28-sim-sale3'
PHONETIC_ALIASES = {
    # Apple / iPhone
    "айфон": "iphone",
    "айфоны": "iphone",
    "айфона": "iphone",
    "айфоне": "iphone",
    "айфоном": "iphone",
    "ифон": "iphone",
    "эпл": "apple",
    "аппл": "apple",
    "макбук": "macbook",
    "макбуки": "macbook",
    "макбука": "macbook",
    "мак": "mac",
    "айпад": "ipad",
    "айпэд": "ipad",
    "эйрподс": "airpods",
    "аирподс": "airpods",
    "эпл вотч": "apple watch",
    "апл вотч": "apple watch",

    # Google
    "гугл": "google",
    "пиксель": "pixel",
    "пиксел": "pixel",
    "пиксельфон": "pixel",

    # Samsung
    "самсунг": "samsung",
    "сасунг": "samsung",
    "самс": "samsung",
    "галакси": "galaxy",
    "гэлэкси": "galaxy",

    # Xiaomi / Redmi / Poco
    "сяоми": "xiaomi",
    "ксиаоми": "xiaomi",
    "редми": "redmi",
    "поко": "poco",

    # Huawei / Honor
    "хуавей": "huawei",
    "хуавэй": "huawei",
    "хонор": "honor",

    # Sony / consoles
    "сони": "sony",
    "плейстейшен": "playstation",
    "плейстейшн": "playstation",
    "плейстейция": "playstation",
    "плойка": "playstation",
    "пс5": "playstation 5",
    "пс 5": "playstation 5",
    "пс4": "playstation 4",
    "пс 4": "playstation 4",
    "ps5 pro": "playstation 5 pro",
    "ps 5 pro": "playstation 5 pro",
    "ps5": "playstation 5",
    "ps 5": "playstation 5",
    "ps4": "playstation 4",
    "ps 4": "playstation 4",

    # Microsoft / Xbox
    "майкрософт": "microsoft",
    "иксбокс": "xbox",
    "хбокс": "xbox",

    # Fitbit
    "фитбит": "fitbit",

    # Ray-Ban / Meta glasses
    # ВАЖНО: приводим к ОДНОМУ токену "rayban".
    # Иначе "ray ban meta" мог случайно совпасть с Apple Watch "... sport band ...",
    # потому что fuzzy видел ban ~ band.
    "ray-ban": "rayban",
    "ray ban": "rayban",
    "rayban": "rayban",
    "рейбан": "rayban",
    "рей бан": "rayban",
    "рэйбан": "rayban",
    "рэй бан": "rayban",

    # Other common brands
    "леново": "lenovo",
    "асус": "asus",
    "эйсус": "asus",
    "асер": "acer",
    "делл": "dell",
    "рейзер": "razer",
    "гармин": "garmin",
    "гопро": "gopro",
    "нинтендо": "nintendo",
}


# Синонимы категорий техники.
TECH_ALIASES = {
    # Phones
    "смартфон": "phone",
    "телефон": "phone",
    "мобильник": "phone",
    "mobile phone": "phone",
    "smartphone": "phone",

    # Laptops
    "ноут": "laptop",
    "ноутбук": "laptop",
    "ноутбуки": "laptop",
    "notebook": "laptop",

    # Tablets
    "планшет": "tablet",
    "таблет": "tablet",

    # Headphones
    "наушники": "headphones",
    "наушник": "headphones",
    "earphones": "headphones",
    "earbuds": "headphones",
    "buds": "headphones",

    # Watches
    "смарт часы": "smartwatch",
    "смарт-часы": "smartwatch",
    "умные часы": "smartwatch",
    "smart watch": "smartwatch",

    # Consoles
    "игровая приставка": "console",
    "приставка": "console",
    "консоль": "console",

    # PC parts
    "видеокарта": "gpu",
    "видюха": "gpu",
    "graphics card": "gpu",
    "процессор": "cpu",
    "проц": "cpu",
    "материнка": "motherboard",
    "материнская плата": "motherboard",
    "оперативка": "ram",
    "оперативная память": "ram",

    # Displays
    "монитор": "display",
    "экран": "display",
}


# Цвета. Каноническое значение справа.
# Маркетинговые оттенки сопоставлены с ближайшим базовым цветом.
COLOR_ALIASES = {
    # blue
    "синий": "blue",
    "синяя": "blue",
    "синее": "blue",
    "синие": "blue",
    "голубой": "blue",
    "голубая": "blue",
    "голубое": "blue",
    "голубые": "blue",
    "navy": "blue",
    "navy blue": "blue",
    "dark blue": "blue",
    "light blue": "blue",
    "sky blue": "blue",
    "azure": "blue",
    "ultramarine": "blue",

    # black
    "черный": "black",
    "чёрный": "black",
    "черная": "black",
    "чёрная": "black",
    "черное": "black",
    "чёрное": "black",
    "черные": "black",
    "чёрные": "black",
    "jet black": "black",
    "midnight": "black",
    "midnight black": "black",

    # white
    "белый": "white",
    "белая": "white",
    "белое": "white",
    "белые": "white",
    "snow": "white",
    "pearl white": "white",
    "ceramic white": "white",

    # gray / graphite
    "серый": "gray",
    "серая": "gray",
    "серое": "gray",
    "серые": "gray",
    "grey": "gray",
    "graphite": "gray",
    "графит": "gray",
    "графитовый": "gray",
    "space gray": "gray",
    "space grey": "gray",
    "spacegray": "gray",

    # silver
    "серебристый": "silver",
    "серебряный": "silver",
    "серебро": "silver",

    # gold
    "золотой": "gold",
    "золотистый": "gold",
    "золото": "gold",
    "champagne": "gold",
    "champagne gold": "gold",

    # rose / pink
    "розовый": "pink",
    "розовая": "pink",
    "розовое": "pink",
    "розовые": "pink",
    "rose": "pink",
    "rose gold": "pink",
    "розовое золото": "pink",

    # red
    "красный": "red",
    "красная": "red",
    "красное": "red",
    "красные": "red",
    "product red": "red",
    "(product) red": "red",
    "бордовый": "red",
    "burgundy": "red",

    # green
    "зеленый": "green",
    "зелёный": "green",
    "зеленая": "green",
    "зелёная": "green",
    "зеленое": "green",
    "зелёное": "green",
    "mint": "green",
    "mint green": "green",
    "мятный": "green",
    "olive": "green",
    "оливковый": "green",

    # purple
    "фиолетовый": "purple",
    "фиолетовая": "purple",
    "лиловый": "purple",
    "violet": "purple",
    "lavender": "purple",
    "лавандовый": "purple",
    "deep purple": "purple",

    # yellow
    "желтый": "yellow",
    "жёлтый": "yellow",
    "желтая": "yellow",
    "жёлтая": "yellow",
    "лимонный": "yellow",
    "lemon": "yellow",

    # orange
    "оранжевый": "orange",
    "оранжевая": "orange",
    "coral": "orange",
    "коралловый": "orange",

    # brown
    "коричневый": "brown",
    "коричневая": "brown",
    "chocolate": "brown",

    # beige / cream
    "бежевый": "beige",
    "беж": "beige",
    "кремовый": "beige",
    "cream": "beige",
    "starlight": "beige",

    # titanium shades
    "natural titanium": "titanium",
    "натуральный титан": "titanium",
    "титан": "titanium",
    "titanium": "titanium",
}


# Состояние товара.
CONDITION_ALIASES = {
    # new / sealed
    "новый": "condnew",
    "новая": "condnew",
    "новое": "condnew",
    "новые": "condnew",
    "new": "condnew",
    "brand new": "condnew",
    "абсолютно новый": "condnew",
    "запечатан": "condnew",
    "запечатанный": "condnew",
    "запечатанная": "condnew",
    "sealed": "condnew",
    "factory sealed": "condnew",
    "не вскрывался": "condnew",
    "не вскрыт": "condnew",
    "unopened": "condnew",

    # used
    "бу": "condused",
    "б/у": "condused",
    "б у": "condused",
    "used": "condused",
    "пользованный": "condused",
    "пользовался": "condused",

    # like new
    "как новый": "condlikenew",
    "как новая": "condlikenew",
    "like new": "condlikenew",
    "идеал": "condlikenew",
    "идеальное состояние": "condlikenew",
    "mint condition": "condlikenew",

    # refurbished
    "восстановленный": "condrefurb",
    "восстановлен": "condrefurb",
    "реф": "condrefurb",
    "refurb": "condrefurb",
    "refurbished": "condrefurb",
}


# Память/накопитель: унифицируем написание единиц.
MEMORY_ALIASES = {
    "гб": "gb",
    "гбайт": "gb",
    "гигабайт": "gb",
    "гигабайта": "gb",
    "гигабайтов": "gb",
    "гиг": "gb",
    "гига": "gb",
    "gbyte": "gb",
    "gigabyte": "gb",
    "gigabytes": "gb",

    "тб": "tb",
    "терабайт": "tb",
    "терабайта": "tb",
    "терабайтов": "tb",
    "тера": "tb",
    "terabyte": "tb",
    "terabytes": "tb",

    "мб": "mb",
    "мегабайт": "mb",
    "megabyte": "mb",
}


# SIM / connectivity.
CONNECTIVITY_ALIASES = {
    "e sim": "esim",
    "nano sim": "sim",
    "nano-sim": "sim",
    "nanosim": "sim",
    "physical sim": "sim",
    "физическая сим": "sim",
    "е-сим": "esim",
    "е сим": "esim",
    "e-sim": "esim",
    "электронная сим": "esim",

    "дуал сим": "dualsim",
    "dual sim": "dualsim",
    "dual-sim": "dualsim",
    "2 sim": "dualsim",
    "2 сим": "dualsim",
    "две сим": "dualsim",

    "вайфай": "wifi",
    "wi-fi": "wifi",
    "wifi only": "wifionly",
    "только wifi": "wifionly",

    "cellular": "cellular",
    "lte": "cellular",
    "4g": "cellular",
}


BRAND_WORDS = {
    "apple",
    "google",
    "samsung",
    "sony",
    "microsoft",
    "xiaomi",
    "huawei",
    "honor",
    "lenovo",
    "asus",
    "acer",
    "dell",
    "hp",
    "lg",
    "meta",
    "rayban",
    "fitbit",
    "garmin",
    "razer",
    "nintendo",
    "oneplus",
    "nothing",
    "dji",
    "gopro",
    "valve",
    "nvidia",
    "amd",
    "intel",
}

# ============================================================
# STRICT PRODUCT IDENTITY
# ============================================================
#
# Fuzzy-поиск хорош для опечаток, но он не должен решать,
# КАКОЙ ЭТО БРЕНД/ЛИНЕЙКА.
#
# Поэтому перед fuzzy-score ставим "identity gate":
# если в запросе явно указан бренд, найденное сообщение обязано
# содержать этот бренд ИЛИ продукт, однозначно его подразумевающий.
#
# Примеры:
#   ray-ban meta -> Meta Quest                      ❌
#   ray-ban meta -> Apple Watch ... sport band    ❌
#   google pixel -> Pixel 9                       ✅
#   apple iphone -> iPhone 17                     ✅
#   sony playstation -> PlayStation 5             ✅

EXPLICIT_BRAND_TOKENS = {
    "apple": {"apple"},
    "google": {"google"},
    "samsung": {"samsung"},
    "sony": {"sony"},
    "microsoft": {"microsoft"},
    "xiaomi": {"xiaomi"},
    "huawei": {"huawei"},
    "honor": {"honor"},
    "lenovo": {"lenovo"},
    "asus": {"asus"},
    "acer": {"acer"},
    "dell": {"dell"},
    "hp": {"hp"},
    "lg": {"lg"},
    "meta": {"meta"},
    "rayban": {"rayban"},
    "fitbit": {"fitbit"},
    "garmin": {"garmin"},
    "razer": {"razer"},
    "nintendo": {"nintendo"},
    "oneplus": {"oneplus"},
    "nothing": {"nothing"},
    "dji": {"dji"},
    "gopro": {"gopro"},
    "valve": {"valve"},
    "nvidia": {"nvidia"},
    "amd": {"amd"},
    "intel": {"intel"},
}

# Токены, которые достаточно однозначно показывают бренд даже если
# само имя бренда продавец не написал.
BRAND_PRESENCE_TOKENS = {
    "apple": {
        "apple", "iphone", "ipad", "macbook", "airpods", "imac",
    },
    "google": {
        "google", "pixel", "fitbit", "nest",
    },
    "samsung": {
        "samsung", "galaxy",
    },
    "sony": {
        "sony", "playstation", "xperia",
    },
    "microsoft": {
        "microsoft", "xbox", "surface",
    },
    "xiaomi": {
        "xiaomi", "redmi", "poco",
    },
    "huawei": {
        "huawei", "matebook",
    },
    "honor": {
        "honor",
    },
    "lenovo": {
        "lenovo", "thinkpad", "legion",
    },
    "asus": {
        "asus", "rog", "zenbook", "vivobook",
    },
    "acer": {
        "acer", "predator",
    },
    "dell": {
        "dell", "alienware", "xps",
    },
    "hp": {
        "hp", "omen", "spectre", "elitebook",
    },
    "lg": {
        "lg",
    },
    "meta": {
        "meta", "quest", "oculus",
    },
    "rayban": {
        "rayban",
    },
    "fitbit": {
        "fitbit",
    },
    "garmin": {
        "garmin",
    },
    "razer": {
        "razer",
    },
    "nintendo": {
        "nintendo", "switch",
    },
    "oneplus": {
        "oneplus",
    },
    "nothing": {
        "nothing",
    },
    "dji": {
        "dji",
    },
    "gopro": {
        "gopro",
    },
    "valve": {
        "valve", "steamdeck",
    },
    "nvidia": {
        "nvidia", "geforce", "rtx", "gtx",
    },
    "amd": {
        "amd", "radeon", "ryzen",
    },
    "intel": {
        "intel",
    },
}

# Слова, которые сами по себе слишком общие и не должны становиться
# "якорем идентичности" товара.
GENERIC_PRODUCT_WORDS = {
    "phone",
    "laptop",
    "tablet",
    "headphones",
    "smartwatch",
    "console",
    "display",
    "gpu",
    "cpu",
    "ram",
    "motherboard",
    "watch",
    "glasses",
    "smart",
    "device",
    "model",
}

# Продуктовые слова, для которых мы хотим требовать совпадение,
# если они есть в запросе.
#
# Это не полный список моделей в мире: все НЕ-общие слова запроса
# автоматически тоже считаются якорями ниже.
KNOWN_PRODUCT_FAMILIES = {
    "iphone",
    "ipad",
    "macbook",
    "airpods",
    "pixel",
    "fitbit",
    "galaxy",
    "playstation",
    "xbox",
    "surface",
    "quest",
    "xperia",
    "redmi",
    "poco",
    "thinkpad",
    "legion",
    "zenbook",
    "vivobook",
    "predator",
    "alienware",
    "steamdeck",
}

# Название семейства можно опустить только там, где продавцы
# действительно регулярно пишут модель без самого семейства.
#
# ВАЖНО:
# PlayStation сюда НЕ входит. "5 Pro" слишком общее сочетание и
# раньше из-за него запрос PlayStation 5 Pro мог цеплять MacBook Pro M5.
OMITTABLE_IDENTITY_FAMILIES = {
    "iphone",
}

# Если в сообщении явно написано ДРУГОЕ семейство товара,
# даже iPhone-сокращение не разрешаем.
IDENTITY_FAMILY_CONFLICTS = (
    KNOWN_PRODUCT_FAMILIES
    | {
        "mac",
        "watch",
        "applewatch",
    }
)

MODEL_TIER_ALIASES = {
    "promax": "pro max",
    "pro-max": "pro max",
    "про макс": "pro max",
    "про-макс": "pro max",
    "макс": "max",
    "про": "pro",
    "ультра": "ultra",
    "плюс": "plus",
    "мини": "mini",
    "лайт": "lite",
    "эйр": "air",
    "аир": "air",
    "фолд": "fold",
    "флип": "flip",
}

MODEL_MODIFIERS = {
    "pro",
    "max",
    "mini",
    "air",
    "ultra",
    "plus",
    "lite",
    "classic",
    "standard",
    "edition",
    "series",
    "gen",
    "generation",
    "se",
    "fold",
    "flip",
    "edge",
}

# Если такая модификация явно указана в запросе, она обязана
# присутствовать в найденном сообщении.
#
# Например:
# iPhone 17 Pro Max -> обязательно и Pro, и Max.
# Samsung S25 Ultra -> обязательно Ultra.
#
# Air тоже строгий для большинства техники, но для Fitbit оставляем
# старое поведение проекта: "Google Fitbit Air" может поймать просто Fitbit.
STRICT_MODEL_MODIFIERS = {
    "pro",
    "max",
    "mini",
    "air",
    "ultra",
    "plus",
    "lite",
    "classic",
    "se",
    "fold",
    "flip",
    "edge",
}

OPTIONAL_MODIFIERS_BY_PRODUCT = {
    "fitbit": {"air"},
}

CANONICAL_CONNECTIVITY = {
    "esim",
    "dualsim",
    "wifi",
    "wifionly",
    "cellular",
}

CANONICAL_COLORS = {
    "blue",
    "black",
    "white",
    "gray",
    "silver",
    "gold",
    "pink",
    "red",
    "green",
    "purple",
    "yellow",
    "orange",
    "brown",
    "beige",
    "titanium",
}

CANONICAL_CONDITIONS = {
    "condnew",
    "condused",
    "condlikenew",
    "condrefurb",
}

ATTRIBUTE_WORDS = (
    CANONICAL_COLORS
    | CANONICAL_CONDITIONS
    | {
        "gb",
        "tb",
        "mb",
        "esim",
        "dualsim",
        "wifi",
        "wifionly",
        "cellular",
        "sim",
    }
)


def _replace_aliases(text: str, mapping: dict[str, str]) -> str:
    """
    Заменяем сначала длинные фразы, потом короткие.
    Границы построены через буквенно-цифровые символы,
    поэтому 'синий' не заменится внутри другого слова.
    """
    for source in sorted(
        mapping,
        key=len,
        reverse=True,
    ):
        target = mapping[source]

        pattern = (
            rf"(?<![\w])"
            rf"{re.escape(source)}"
            rf"(?![\w])"
        )

        text = re.sub(
            pattern,
            target,
            text,
            flags=re.IGNORECASE,
        )

    return text


@lru_cache(maxsize=8192)
def normalize_text(text: str) -> str:
    if not text:
        return ""

    text = text.casefold().replace(
        "ё",
        "е",
    )

    # Preserve Russian-only markers before transliteration splits «только».
    esim_word = r'(?:e[ -]?sim|е[ -]?сим|есим)'
    text = re.sub(r'(?<!\w)(?:только|онли)(?!\w)(?=\s+' + esim_word + r'\b)', 'only', text)
    text = re.sub(r'(?<!\w)(' + esim_word + r')\s+(?:только|онли)(?!\w)', r'\1 only', text)

    text = re.sub(r"(?<!\w)с\s*(?=\d{2}\b)", "s ", text)
    text = re.sub(r"([^\W\d_])(\d)", r"\1 \2", text)
    text = re.sub(r"(\d)([^\W\d_])", r"\1 \2", text)

    # Сначала фразы и смысловые синонимы.
    text = _replace_aliases(
        text,
        CONDITION_ALIASES,
    )
    text = _replace_aliases(
        text,
        COLOR_ALIASES,
    )
    text = _replace_aliases(
        text,
        MEMORY_ALIASES,
    )
    text = _replace_aliases(
        text,
        CONNECTIVITY_ALIASES,
    )
    text = _replace_aliases(
        text,
        TECH_ALIASES,
    )
    text = _replace_aliases(
        text,
        MODEL_TIER_ALIASES,
    )
    text = _replace_aliases(
        text,
        PHONETIC_ALIASES,
    )

    # Остаток кириллицы -> латиница.
    text = unidecode(
        text
    )

    # Samsung часто пишут S26+ вместо S26 Plus.
    # Сохраняем "+" как модификацию модели, а не выбрасываем.
    text = re.sub(
        r"(?<=\d)\s*\+",
        " plus ",
        text,
    )

    # iphone17 -> iphone 17
    # 256gb -> 256 gb
    text = re.sub(
        r"([a-z])(\d)",
        r"\1 \2",
        text,
    )
    text = re.sub(
        r"(\d)([a-z])",
        r"\1 \2",
        text,
    )

    text = re.sub(
        r"[^a-z0-9]+",
        " ",
        text,
    )

    return " ".join(
        text.split()
    )


def numeric_tokens(text: str) -> list[str]:
    return re.findall(
        r"\d+",
        normalize_text(text),
    )


def token_match_score(
    needle: str,
    hay_tokens: list[str],
) -> float:
    if not needle or not hay_tokens:
        return 0.0

    return max(
        fuzz.ratio(
            needle,
            token,
        )
        for token in hay_tokens
    )


def extract_color_constraints(
    normalized_text: str,
) -> set[str]:
    tokens = set(
        normalized_text.split()
    )

    return (
        tokens
        & CANONICAL_COLORS
    )


def extract_condition_constraints(
    normalized_text: str,
) -> set[str]:
    tokens = set(
        normalized_text.split()
    )

    return (
        tokens
        & CANONICAL_CONDITIONS
    )


def _fuzzy_token_present(
    expected: str,
    message_tokens: list[str],
) -> bool:
    """
    Для коротких названий брендов требуем точное совпадение.
    Для длинных разрешаем небольшую опечатку.
    """
    if not expected:
        return False

    if expected in message_tokens:
        return True

    if len(expected) <= 4:
        return False

    return any(
        fuzz.ratio(expected, token) >= 90
        for token in message_tokens
        if len(token) >= 4
    )


def extract_explicit_brand_constraints(
    normalized_query: str,
) -> set[str]:
    query_tokens = set(
        normalized_query.split()
    )

    required = set()

    for brand, explicit_tokens in EXPLICIT_BRAND_TOKENS.items():
        if query_tokens & explicit_tokens:
            required.add(brand)

    return required


def brand_is_present(
    brand: str,
    normalized_message: str,
) -> bool:
    message_tokens = normalized_message.split()

    allowed = BRAND_PRESENCE_TOKENS.get(
        brand,
        {brand},
    )

    for token in allowed:
        if _fuzzy_token_present(
            token,
            message_tokens,
        ):
            return True

    return False


def extract_identity_anchors(
    normalized_query: str,
) -> list[str]:
    """
    Возвращает смысловые якоря товара после удаления:
    - явных брендов
    - цветов/состояния/памяти/SIM
    - модификаторов Pro/Max/Ultra и т.д.
    - цифр модели (они проверяются отдельно)

    Для многословного товара каждый такой якорь должен реально
    присутствовать в сообщении (с небольшой терпимостью к опечаткам).
    """
    query_tokens = sim_identity_text(normalized_query).split()

    explicit_brand_words = set()

    for tokens in EXPLICIT_BRAND_TOKENS.values():
        explicit_brand_words |= tokens

    anchors = []

    for token in query_tokens:
        if token.isdigit():
            continue

        if token in explicit_brand_words:
            continue

        if token in ATTRIBUTE_WORDS:
            continue

        if token in MODEL_MODIFIERS:
            continue

        if token in GENERIC_PRODUCT_WORDS:
            continue

        if len(token) < 3:
            continue

        anchors.append(token)

    return anchors


def identity_anchor_present(
    anchor: str,
    normalized_message: str,
) -> bool:
    message_tokens = normalized_message.split()

    if anchor in message_tokens:
        return True

    # Для коротких слов fuzzy опасен: ban/band и подобные случаи.
    if len(anchor) <= 4:
        return False

    # Для длинных названий допускаем обычную опечатку.
    return any(
        fuzz.ratio(anchor, token) >= 86
        for token in message_tokens
        if len(token) >= 4
    )


def has_strong_structured_signature(
    normalized_query: str,
    normalized_message: str,
) -> bool:
    """
    Разрешает опустить название семейства товара только когда
    остальная сигнатура очень специфична.

    Пример:
      запрос:  iPhone 17 Pro Max eSim 512
      сообщение: 17 Pro Max 512 eSim
    Это всё ещё достаточно однозначно.

    Одного числа или одного слова для такого исключения недостаточно.
    """
    score = 0

    q_storage = extract_storage_constraints(
        normalized_query
    )
    m_storage = extract_storage_constraints(
        normalized_message
    )

    if q_storage and storage_equivalent(
        q_storage,
        m_storage,
    ):
        score += 1

    q_modifiers = extract_model_modifier_constraints(
        normalized_query
    )
    m_modifiers = extract_model_modifier_constraints(
        normalized_message
    )

    if q_modifiers and q_modifiers.issubset(
        m_modifiers
    ):
        score += 1

    q_connectivity = extract_connectivity_constraints(
        normalized_query
    )
    m_connectivity = extract_connectivity_constraints(
        normalized_message
    )

    if q_connectivity and q_connectivity.issubset(
        m_connectivity
    ):
        score += 1

    q_colors = extract_color_constraints(
        normalized_query
    )
    m_colors = extract_color_constraints(
        normalized_message
    )

    if q_colors and (q_colors & m_colors):
        score += 1

    storage_numbers = {
        str(value)
        for value, _ in q_storage
    }

    q_model_numbers = [
        n
        for n in identity_numbers(
            normalized_query
        )
        if n not in storage_numbers
    ]

    m_numbers = set(
        numeric_tokens(
            normalized_message
        )
    )

    if (
        q_model_numbers
        and all(
            n in m_numbers
            for n in q_model_numbers
        )
    ):
        score += 1

    return score >= 2


def can_omit_identity_anchor(
    anchor: str,
    normalized_query: str,
    normalized_message: str,
) -> bool:
    """
    Безопасное исключение для сокращённых названий.

    Пример разрешён:
      iPhone 17 Pro Max 512 eSIM
      -> "17 Pro Max 512 eSIM"

    Пример ЗАПРЕЩЁН:
      PlayStation 5 Pro
      -> "MacBook Pro M5"

    Раньше здесь было слишком общее правило:
    число + Pro считались "сильной сигнатурой" для ЛЮБОГО товара.
    Именно из-за этого PlayStation мог превращаться в MacBook.
    """
    if anchor not in OMITTABLE_IDENTITY_FAMILIES:
        return False

    message_tokens = set(
        normalized_message.split()
    )

    # A short iPhone name may omit the brand, but an explicit competing
    # manufacturer never becomes Apple through the structural exception.
    if any(brand_is_present(brand, normalized_message) for brand in EXPLICIT_BRAND_TOKENS if brand != 'apple'):
        return False

    conflicting = (
        IDENTITY_FAMILY_CONFLICTS
        - {anchor}
    )

    if message_tokens & conflicting:
        return False

    return has_strong_structured_signature(
        normalized_query,
        normalized_message,
    )


def passes_product_identity_gate(
    normalized_query: str,
    normalized_message: str,
) -> bool:
    """
    Строгий фильтр идентичности ДО fuzzy-score.
    """
    # 1. Все явно указанные бренды обязательны.
    required_brands = extract_explicit_brand_constraints(
        normalized_query
    )

    if product_kind(normalized_query) != product_kind(normalized_message):
        return False
    if product_kind(normalized_query) == 'device' and set(normalized_query.split()) & {'iphone', 'ipad', 'macbook', 'airpods'}:
        if any(brand_is_present(brand, normalized_message) for brand in EXPLICIT_BRAND_TOKENS if brand != 'apple'):
            return False

    for brand in required_brands:
        if not brand_is_present(
            brand,
            normalized_message,
        ):
            return False

    # 2. Смысловые якоря товара тоже обязательны.
    #
    # Например:
    # Meta Quest -> Quest обязателен.
    # Поэтому Ray-Ban Meta не станет Meta Quest и наоборот.
    anchors = extract_identity_anchors(
        normalized_query
    )

    for anchor in anchors:
        if identity_anchor_present(
            anchor,
            normalized_message,
        ):
            continue

        # Иногда продавец пишет только:
        # "17 Pro Max 512 eSIM" без слова iPhone.
        # Разрешаем такое ТОЛЬКО для известного семейства товара
        # и только при сильной структурной сигнатуре.
        if can_omit_identity_anchor(
            anchor,
            normalized_query,
            normalized_message,
        ):
            continue

        return False

    return True


def extract_compact_model_codes(
    normalized_text: str,
) -> set[str]:
    """
    Распознаёт короткие буквенно-цифровые модели после normalize_text.

    Примеры:
      S26   -> "s 26" -> {"s26"}
      A56   -> "a 56" -> {"a56"}
      M4    -> "m 4"  -> {"m4"}

    Это не даёт S26 случайно совпасть с A26 или iPhone 26.
    """
    tokens = normalized_text.split()
    result = set()

    for index in range(
        len(tokens) - 1
    ):
        prefix = tokens[index]
        number = tokens[index + 1]

        if (
            len(prefix) == 1
            and prefix.isalpha()
            and number.isdigit()
            and 1 <= len(number) <= 4
        ):
            result.add(
                f"{prefix}{number}"
            )

    return result


def samsung_s_series_codes(
    normalized_text: str,
) -> set[str]:
    """
    Samsung Galaxy S-series: S24, S25, S26 и т.д.
    """
    return {
        code
        for code in extract_compact_model_codes(
            normalized_text
        )
        if re.fullmatch(
            r"s\d{1,3}",
            code,
        )
    }


STRICT_BASE_VARIANT_FAMILIES = {
    "iphone",
    "ipad",
    "macbook",
    "pixel",
    "galaxy",
    "playstation",
    "xbox",
    "surface",
    "quest",
    "xperia",
}


def enforce_numbered_family_variant(
    normalized_query: str,
    normalized_message: str,
) -> bool:
    """
    Если пользователь запросил базовую НУМЕРОВАННУЮ модель,
    версия Pro/Plus/Ultra/Max/etc считается другим товаром.

    Примеры:
      PlayStation 5       -> PlayStation 5 Pro   ❌
      iPhone 17           -> iPhone 17 Pro       ❌
      Pixel 9             -> Pixel 9 Pro         ❌
      PlayStation 5 Pro   -> PlayStation 5 Pro   ✅

    Для широкого запроса без номера ("macbook", "pixel") правило
    не включается.
    """
    q_tokens = set(
        normalized_query.split()
    )
    m_tokens = set(
        normalized_message.split()
    )

    families = (
        q_tokens
        & STRICT_BASE_VARIANT_FAMILIES
    )

    if not families:
        return True

    q_storage = extract_storage_constraints(
        normalized_query
    )
    storage_numbers = {
        str(value)
        for value, _ in q_storage
    }

    q_model_numbers = [
        number
        for number in identity_numbers(
            normalized_query
        )
        if number not in storage_numbers
    ]

    if not q_model_numbers:
        return True

    q_modifiers = extract_model_modifier_constraints(
        normalized_query
    )
    m_modifiers = extract_model_modifier_constraints(
        normalized_message
    )

    return q_modifiers == m_modifiers


def enforce_exact_base_variant(
    normalized_query: str,
    normalized_message: str,
) -> bool:
    """
    Для Samsung S-series базовая модель считается отдельной версией.

    Запрос:
      S26
      S26 Black

    НЕ должен ловить:
      S26 Ultra
      S26 Plus
      S26+
      S26 Edge

    Но:
      S26 Ultra -> S26 Ultra ✅
      S26 Plus  -> S26 Plus  ✅
    """
    q_codes = samsung_s_series_codes(
        normalized_query
    )

    if not q_codes:
        return True

    m_codes = samsung_s_series_codes(
        normalized_message
    )

    # Должна быть именно та же S-модель.
    if not q_codes.issubset(
        m_codes
    ):
        return False

    q_modifiers = extract_model_modifier_constraints(
        normalized_query
    )
    m_modifiers = extract_model_modifier_constraints(
        normalized_message
    )

    # Если версия явно указана в запросе — она обязательна.
    if q_modifiers:
        return q_modifiers == m_modifiers

    # Если запрос на ОБЫЧНЫЙ S26, то Ultra/Plus/Edge и т.п.
    # уже другой товар и не подходит.
    extra_variant_modifiers = (
        m_modifiers
        & {
            "ultra",
            "plus",
            "edge",
            "pro",
            "max",
            "mini",
            "lite",
        }
    )

    return not extra_variant_modifiers


def extract_model_modifier_constraints(
    normalized_text: str,
) -> set[str]:
    tokens = set(
        normalized_text.split()
    )

    required = (
        tokens
        & STRICT_MODEL_MODIFIERS
    )

    # Сохраняем наше старое специальное поведение:
    # Google Fitbit Air -> Fitbit тоже допустимо.
    for product, optional_modifiers in OPTIONAL_MODIFIERS_BY_PRODUCT.items():
        if product in tokens:
            required -= optional_modifiers

    return required


def extract_connectivity_constraints(
    normalized_text: str,
) -> set[str]:
    tokens = set(
        normalized_text.split()
    )

    return (
        tokens
        & CANONICAL_CONNECTIVITY
    ) - {'esim', 'dualsim'}


def sim_spec(text: str) -> dict:
    tokens = set(text.split())
    no_esim_pattern = r'\b(?:bez|without|no)\s+esim\b'
    no_physical_pattern = r'\b(?:bez|without|no)\s+(?:(?:physical|fizichesk\w*)\s+)?sim\b'
    no_esim = bool(re.search(no_esim_pattern + r'|\bsim\s+only\b', text))
    no_physical = bool(re.search(no_physical_pattern, text))
    only_esim = bool(re.search(r'\b(?:(?:only|tolko)\s+esim|esim\s+only)\b', text))
    positive_tokens = set(re.sub(no_esim_pattern + '|' + no_physical_pattern, ' ', text).split())
    physical = bool(positive_tokens & {'sim', 'dualsim'})
    esim = 'esim' in positive_tokens
    return dict(physical=physical, esim=esim, no_esim=no_esim,
                no_physical=no_physical or only_esim,
                dual='dualsim' in tokens,
                single=bool(re.search(r'\b(?:1|single|odna)\s+sim\b', text)),
                global_label=bool(re.search(r'\b(?:global\w*|glob)\b', text)),
                esim_count=next((2 if m in {'2', 'dva', 'dve', 'dual'} else 1
                                 for m in re.findall(r'\b(1|2|dva|dve|dual|odna|single)\s+esim\b', text)), None),
                valid=not ((physical and (no_physical or only_esim)) or (esim and no_esim)),
                known=bool(tokens & {'sim', 'esim', 'dualsim'}))


def sim_compatible(query: str, message: str) -> bool:
    expected, actual = sim_spec(query), sim_spec(message)
    if not expected['valid'] or not actual['valid']:
        return False
    if not expected['known']:
        return True
    if not actual['known']:
        return False
    if expected['physical'] and not actual['physical']:
        return False
    if expected['esim'] and not actual['esim']:
        return False
    if expected['global_label'] and not actual['global_label']:
        return False
    requires_only = (expected['no_physical'] or
                     (expected['esim'] and not expected['physical'] and not expected['global_label']))
    # In a price query a standalone eSIM means the eSIM-only version.
    if requires_only and actual['physical']:
        return False
    # A global label plus eSIM does not prove that the physical slot is absent.
    # An explicit eSIM-only statement is still usable, regardless of region.
    if requires_only and actual['global_label'] and not actual['no_physical']:
        return False
    if expected['esim_count'] is not None and expected['esim_count'] != actual['esim_count']:
        return False
    if expected['no_esim'] and actual['esim']:
        return False
    if expected['dual'] and not actual['dual']:
        return False
    if expected['single'] and actual['dual']:
        return False
    return True


def identity_numbers(text: str) -> list[str]:
    # SIM counts are attributes, not device model numbers.
    return numeric_tokens(re.sub(r'\b[12]\s+(?:sim|esim)\b', 'sim', text))


def sim_identity_text(text: str) -> str:
    # SIM operators are validated by sim_compatible, not as product names.
    text = re.sub(r'\b(?:bez|without|no)\s+(?:(?:physical|fizichesk\w*)\s+)?(?=sim\b|esim\b)', '', text)
    text = re.sub(r'\b(?:single|odna)\s+(?=sim\b)|\b(?:tolko|only)\s+(?=esim\b)', '', text)
    return re.sub(r'(?<=sim)\s+only\b', '', text)


def extract_storage_constraints(
    normalized_text: str,
) -> set[tuple[int, str]]:
    """
    Из:
      256 gb
      1 tb
    делаем:
      {(256, "gb")}
      {(1, "tb")}
    """
    matches = re.findall(
        r"\b(\d{1,4})\s*(gb|tb|mb)\b",
        normalized_text,
    )

    result = {
        (
            int(value),
            unit,
        )
        for value, unit in matches
    }
    # A bare capacity in a phone request is common: "iPhone 17 256".
    # Infer only conventional capacities and only in a known phone context.
    if (re.search(r"\b(?:iphone|pixel|samsung|galaxy|oneplus|honor|xiaomi|redmi|poco|nothing|vivo|realme)\b|\b[sa]\s+\d{2}\b", normalized_text)
            or (not extract_explicit_brand_constraints(normalized_text)
                and re.search(r"\b(?:1[0-9]|20)\s+(?:pro|max|mini|plus)\b", normalized_text))):
        model_numbers = set(re.findall(r"\b(?:iphone|pixel|honor|oneplus|redmi|xiaomi|s|a|x|v|f|m|c)\s+(\d+)\b", normalized_text))
        explicit_numbers = {str(value) for value, _ in result}
        for token in normalized_text.split():
            if token in {"32", "64", "128", "256", "512", "1024", "2048", "4096"} and token not in model_numbers | explicit_numbers:
                result.add((int(token), "gb"))
    return result


def storage_equivalent(
    query_storage: set[tuple[int, str]],
    message_storage: set[tuple[int, str]],
) -> bool:
    if not query_storage:
        return True

    if not message_storage:
        return False

    def to_mb(
        value: int,
        unit: str,
    ) -> int:
        if unit == "tb":
            return value * 1024 * 1024

        if unit == "gb":
            return value * 1024

        return value

    query_mb = {
        to_mb(value, unit)
        for value, unit in query_storage
    }

    message_mb = {
        to_mb(value, unit)
        for value, unit in message_storage
    }

    return query_mb.issubset(message_mb)


def product_match_score(
    query: str,
    message: str,
) -> float:
    q = normalize_text(
        query
    )
    m = normalize_text(
        message
    )

    if not q or not m:
        return 0.0

    if not sim_compatible(q, m):
        return 0.0
    if not role_colors_match(q, m):
        return 0.0
    if not model_variants_match(q, m):
        return 0.0

    q_tokens = q.split()
    m_tokens = m.split()

    # --------------------------------------------------------
    # STRICT PRODUCT IDENTITY GATE
    # --------------------------------------------------------
    #
    # Сначала убеждаемся, что это вообще тот бренд/товар.
    # Только после этого запускаем fuzzy-score.
    if not passes_product_identity_gate(
        q,
        m,
    ):
        return 0.0

    # --------------------------------------------------------
    # ОБЯЗАТЕЛЬНЫЕ ХАРАКТЕРИСТИКИ ИЗ ЗАПРОСА
    # --------------------------------------------------------

    # Базовая нумерованная модель != Pro/Plus/Ultra/Max.
    if not enforce_numbered_family_variant(
        q,
        m,
    ):
        return 0.0

    # Samsung S-series:
    # S26 = только обычный S26.
    # S26 Ultra / Plus / Edge — отдельные версии.
    if not enforce_exact_base_variant(
        q,
        m,
    ):
        return 0.0

    # Если запрос содержит короткий код модели вроде S26/A56/M4,
    # в сообщении должен быть тот же код.
    q_model_codes = extract_compact_model_codes(
        q
    )

    if q_model_codes:
        m_model_codes = extract_compact_model_codes(
            m
        )

        if not q_model_codes.issubset(
            m_model_codes
        ):
            return 0.0

    # Цвет:
    # "iphone 14 blue" НЕ должен ловить "iphone 14 red".
    q_colors = extract_color_constraints(
        q
    )

    if q_colors:
        m_colors = extract_color_constraints(
            m
        )

        if not q_colors.issubset(m_colors):
            return 0.0

    # Состояние:
    # запрос "new" не должен ловить явное "used".
    q_conditions = extract_condition_constraints(
        q
    )

    if q_conditions:
        m_conditions = extract_condition_constraints(
            m
        )

        if not q_conditions.issubset(m_conditions):
            return 0.0

    # Версия / модификация модели.
    #
    # Это исправляет важный кейс:
    # "iPhone 17 Pro Max" больше НЕ ловит обычный "iPhone 17".
    q_modifiers = extract_model_modifier_constraints(
        q
    )

    if q_modifiers:
        m_modifiers = extract_model_modifier_constraints(
            m
        )

        if q_modifiers != m_modifiers:
            return 0.0

    # Тип связи / SIM.
    #
    # Если в запросе явно eSIM, обычный SIM без eSIM больше не подходит.
    q_connectivity = extract_connectivity_constraints(
        q
    )

    if q_connectivity:
        m_connectivity = extract_connectivity_constraints(
            m
        )

        if not q_connectivity.issubset(
            m_connectivity
        ):
            return 0.0

    # Накопитель / память.
    q_storage = extract_storage_constraints(
        q
    )

    if q_storage:
        m_storage = extract_storage_constraints(
            m
        )

        if not storage_equivalent(
            q_storage,
            m_storage,
        ):
            return 0.0

    # --------------------------------------------------------
    # ЦИФРЫ МОДЕЛИ
    # --------------------------------------------------------

    # Цифры, относящиеся к памяти, не считаем "моделью",
    # потому что память уже проверили выше.
    storage_numbers = {
        str(value)
        for value, _ in q_storage
    }

    q_numbers = [
        number
        for number in identity_numbers(q)
        if number not in storage_numbers
    ]

    m_numbers = numeric_tokens(
        m
    )

    # Каждая цифра модели из запроса обязательна.
    # iPhone 17 -> не ловит iPhone 16.
    for number in q_numbers:
        if number not in m_numbers:
            return 0.0

    # После всех строгих проверок точное включение — максимум.
    if q in m:
        return 100.0

    full_partial = fuzz.partial_ratio(
        q,
        m,
    )
    full_token = fuzz.token_set_ratio(
        q,
        m,
    )

    # Если строгий identity gate уже доказал, что это нужный
    # бренд/линейка/семейство, не даём обычному fuzzy-score
    # случайно занизить правильное совпадение.
    identity_floor = 0.0

    if (
        extract_explicit_brand_constraints(q)
        or extract_identity_anchors(q)
        or has_strong_structured_signature(q, m)
    ):
        identity_floor = 90.0

    words = [
        token
        for token in q_tokens
        if not token.isdigit()
    ]

    # Главное название товара.
    # Цвет/память/состояние/модификация/eSIM уже проверены как атрибуты,
    # поэтому не даём им искусственно повышать fuzzy-score.
    core = [
        word
        for word in words
        if word not in BRAND_WORDS
        and word not in MODEL_MODIFIERS
        and word not in ATTRIBUTE_WORDS
        and len(word) >= 3
    ]

    if not core:
        core = [
            word
            for word in words
            if word not in ATTRIBUTE_WORDS
            and len(word) >= 3
        ]

    if not core:
        return max(
            identity_floor,
            full_partial,
            full_token,
        )

    core_scores = [
        token_match_score(
            word,
            m_tokens,
        )
        for word in core
    ]

    best_core = max(
        core_scores
    )
    average_core = (
        sum(core_scores)
        / len(core_scores)
    )

    # Если ядро состоит из одного товара/слова — одного сильного
    # совпадения достаточно.
    #
    # Составные бренды вроде Ray-Ban заранее склеиваются в rayban,
    # чтобы кусок "ban" не совпадал с обычным словом "band".
    # Например Google Fitbit Air -> Fitbit.
    if len(core) == 1:
        if best_core >= 90:
            return max(
                identity_floor,
                90.0,
                full_partial,
                full_token,
            )

        if best_core >= 76:
            return max(
                identity_floor,
                best_core,
                full_partial,
                full_token,
            )

        return max(
            identity_floor,
            full_partial,
            full_token,
        )

    # Для многословного ядра ОДНО совпавшее слово больше не достаточно.
    # Это исправляет:
    # "ray-ban meta" -> Meta Quest  ❌
    #
    # Требуем как минимум два действительно похожих токена.
    strong = sum(
        score >= 78
        for score in core_scores
    )

    if strong >= 2:
        return max(
            identity_floor,
            average_core,
            full_partial,
            full_token,
        )

    # Даже высокий token_set_ratio не должен протащить запрос,
    # если совпало только одно общее слово.
    if identity_floor >= 76:
        return identity_floor

    return min(
        full_partial,
        full_token,
        70.0,
    )



def split_message_into_match_segments(
    message_text: str,
) -> list[str]:
    """
    Разбивает длинный прайс/каталог на отдельные позиции.

    Например:
      S26 black
      S26 blue
      S26 violet
      S26 gold

    будут проверяться как четыре отдельные позиции.

    Также понимаем:
      S26 black / S26 blue
      S26 black | S26 blue
      S26 black ; S26 blue
    """
    text = (
        message_text
        or ""
    ).strip()

    if not text:
        return []

    # Сначала строки.
    raw_lines = re.split(
        r"[\r\n]+",
        text,
    )

    parts = []

    for raw_line in raw_lines:
        line = raw_line.strip(
            " \t•▪▫◦·-–—"
        )

        if not line:
            continue

        # Разделители товарных позиций внутри одной строки.
        subparts = re.split(
            r"\s+(?:\||;|/)\s+",
            line,
        )

        for part in subparts:
            part = part.strip(
                " \t•▪▫◦·-–—"
            )

            if part:
                parts.append(part)

    if not parts:
        return [text]

    # Убираем точные повторы, сохраняя порядок.
    unique = []
    seen = set()

    for part in parts:
        key = part.casefold()

        if key in seen:
            continue

        seen.add(key)
        unique.append(part)

    return unique[:150]


def product_kind(text: str) -> str:
    if re.search(r"\b(?:чехол\w*|chekhol\w*|cover|case\s+for|protective\s+case)\b|^case\b", text):
        return "case"
    if re.search(r"\b(?:iphone|ipad|airpods|buds|pixel)\b.*\bcase\b", text):
        return "case"
    if re.search(r"\b(?:стекл\w*|пленк\w*|steklo|plenka|screen protector|tempered glass)\b", text):
        return "protector"
    if re.search(r"\b(?:заряд\w*|zariad\w*|charger|adapter|cable|кабель|kabel)\b", text):
        return "power accessory"
    if re.search(r"\b(?:ремеш\w*|remesh\w*|strap|replacement band)\b", text):
        return "strap"
    return "device"


def role_colors_match(query: str, message: str) -> bool:
    def roles(text):
        result = {}
        markers = list(re.finditer(r"\b(f|frame|oprava|l|lens|linzy)\b", text))
        for index, marker in enumerate(markers):
            end = markers[index + 1].start() if index + 1 < len(markers) else len(text)
            colors = extract_color_constraints(text[marker.end():end])
            if colors:
                result['frame' if marker[1] in {'f', 'frame', 'oprava'} else 'lens'] = colors
        return result
    expected, actual = roles(query), roles(message)
    return all(value.issubset(actual.get(role, set())) for role, value in expected.items())


def model_variants_match(query: str, message: str) -> bool:
    def models(text):
        return {(family, number): suffix or '' for family, number, suffix in re.findall(
            r"\b(iphone|pixel|oneplus|honor|redmi|xiaomi|nothing)\s+(?:phone\s+)?(\d{1,3})(?:\s+([a-z])\b)?", text)}
    expected, actual = models(query), models(message)
    return all((key not in actual and not suffix) or actual.get(key) == suffix
               for key, suffix in expected.items())


_BUY = re.compile(r"\b(?:куплю|купим|ищу|ищем|нужен|нужна|нужно|нужны|интересует|возьму|возьмем|wtb|buying|looking for)\b", re.I)
_SELL = re.compile(r"\b(?:продам|продаю|продаем|продается|продажа|в наличии|прайс|wts|selling|for sale)\b", re.I)
_NEGATIVE = re.compile(r"\b(?:не (?:нужен|нужна|нужно|нужны|ищу|куплю)|уже (?:купил|нашел)|запрос (?:закрыт|неактуален))\b", re.I)
_PRICE_AMOUNT = r'(?:[1-9]\d{0,2}(?:[\s\u00a0\u202f.,]\d{3}){1,2}|[1-9]\d{3,7}|[1-9]\d{0,2}\s*(?:тыс\.?|[kк]))'
_CURRENCY = r'(?:₽|р\.?|руб(?:\.|лей|ля|ль)?|rur|rub|usd|eur|\$|€)'
_PRICED_OFFER = re.compile(
    r'(?:\s[-–—]\s*(?:[₽$€]\s*)?' + _PRICE_AMOUNT +
    r'(?:\s*' + _CURRENCY + r')?'
    r'|\b(?:цена|price)\s*[:=\-]?\s*(?:[₽$€]\s*)?' + _PRICE_AMOUNT +
    r'(?:\s*' + _CURRENCY + r')?'
    r'|\s' + _PRICE_AMOUNT + r'\s*' + _CURRENCY + r')(?!\w)', re.I)
_BUYER_BUDGET = re.compile(r'\b(?:до|бюджет|не\s+дороже|не\s+выше|не\s+более|максимум)'
                           r'\s*[:=]?\s*(?:[₽$€]\s*)?' + _PRICE_AMOUNT + r'(?!\w)', re.I)


def _intent(text):
    text = text.casefold().replace('ё', 'е')
    if _NEGATIVE.search(text):
        return 'skip'
    buy, sell = bool(_BUY.search(text)), bool(_SELL.search(text))
    if buy and sell:
        return 'skip'
    return 'buy' if buy else 'sell' if sell else None


def _attribute_continuation(text):
    tokens = normalize_text(text).split()
    allowed = ATTRIBUTE_WORDS | STRICT_MODEL_MODIFIERS | {'sim', 'ram', 'rom', 'gb', 'tb', 'mb', 'mm', 'only'}
    return bool(tokens) and all(token.isdigit() or token in allowed for token in tokens)


def best_query_segment(query: str, message_text: str, intent_mode: str = 'buy') -> tuple[float, str]:
    """One matcher for monitoring and /test; never combine two product rows."""
    parts = split_message_into_match_segments(message_text)
    candidates = []
    context = None
    previous = None
    for part in parts:
        explicit = _intent(part)
        if explicit is not None:
            context = explicit
        # An unlabelled product line ending in a price is a sale offer unless
        # the same line or an earlier header explicitly says it is a purchase.
        priced_sale = (explicit is None and context != 'buy' and bool(_PRICED_OFFER.search(part))
                       and not _BUYER_BUDGET.search(part))
        allowed = context != 'skip' and (intent_mode == 'all' or (context != 'sell' and not priced_sale))
        if allowed:
            # Preserve purchase-header context for the later outbox recheck;
            # only the selected segment is persisted for notification delivery.
            selected = ('Куплю: ' + part if context == 'buy' and explicit is None
                        and _PRICED_OFFER.search(part) else part)
            candidates.append(selected)
            if previous is not None and _attribute_continuation(part) and not _attribute_continuation(previous):
                combined = previous + '\n' + part
                if len(combined) <= 500:
                    candidates.append(combined)
        previous = selected if allowed else None
    best_score, best_segment = 0.0, ''
    for candidate in candidates:
        score = product_match_score(query, candidate)
        if score > best_score:
            best_score, best_segment = score, candidate
    return best_score, best_segment
