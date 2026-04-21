"""
ts_crawler.py
GitHub Actions 전용: coinpost.jp + nakaoka-inc.com + senjutsu.jp RSS 크롤링 → Gemini 번역 → Supabase 저장
대상 사이트: t-s.co.kr (roxic.tistory.com)
콘셉트: 운세테크 — 투자 타이밍 + 오행/사주 결합
"""

import os
import json
import feedparser
import logging
import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, Optional
from dotenv import load_dotenv
from supabase import create_client
import google.generativeai as genai

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

# -------------------------------------------------------------------------
# 설정
# -------------------------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

FEED_SOURCES = [
    "https://coinpost.jp/?feed=rss2",
    "https://nakaoka-inc.com/staffblog/feed/",
    "https://senjutsu.jp/feed/",
]

# 이 키워드 중 하나라도 제목에 포함되면 수집
KEYWORDS = [
    # 코인/투자
    "ビットコイン", "BTC", "仮想通貨", "暗号資産", "ETF", "ステーブルコイン",
    "金", "ゴールド", "資産", "投資", "相場", "価格", "上昇", "下落",
    # 점술/운세
    "四柱推命", "占い", "運勢", "運気", "風水", "五行", "オーラ",
    "紫微斗数", "周易", "奇門遁甲", "宿命", "運命", "タイミング",
]

MAX_ARTICLES = 10
TABLE_NAME = "ts_articles"
HISTORY_FILE = "posted_articles_ts.json"

# -------------------------------------------------------------------------
# 소스별 콘텐츠 톤 매핑
# -------------------------------------------------------------------------
SOURCE_MAP = {
    "coinpost.jp": "coin",
    "nakaoka-inc.com": "gold",
    "senjutsu.jp": "fortune",
}

def get_source(url: str) -> str:
    for domain, label in SOURCE_MAP.items():
        if domain in url:
            return label
    return "unknown"

# -------------------------------------------------------------------------
# 저작권 문구 제거
# -------------------------------------------------------------------------
COPYRIGHT_PATTERNS = [
    r'<p[^>]*>©.*?</p>',
    r'<p[^>]*>&copy;.*?</p>',
    r'<p[^>]*>※.*?</p>',
    r'©[^\n<]*',
    r'&copy;[^\n<]*',
    r'ライター：[^\n<]*',
    r'掲載日：[^\n<]*',
]

def remove_copyright(html: str) -> str:
    for pattern in COPYRIGHT_PATTERNS:
        html = re.sub(pattern, '', html, flags=re.DOTALL)
    return html.strip()


def contains_keyword(title: str) -> bool:
    return any(kw in title for kw in KEYWORDS)


class TSCrawler:
    def __init__(self):
        self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        genai.configure(api_key=GEMINI_API_KEY)
        self.model = genai.GenerativeModel(GEMINI_MODEL)

        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                self.posted_articles = json.load(f)
        else:
            self.posted_articles = {}

    # -------------------------------------------------------------------------
    # RSS 수집
    # -------------------------------------------------------------------------
    def collect_entries(self):
        feedparser.USER_AGENT = USER_AGENT
        entries = []
        for url in FEED_SOURCES:
            feed = feedparser.parse(url)
            logger.info(f"[RSS] {url} → {len(feed.entries)}개")
            for e in feed.entries:
                if e.link in self.posted_articles:
                    continue
                # senjutsu는 키워드 필터 없이 전부 수집 (점술 전문 사이트)
                if "senjutsu.jp" in url:
                    entries.append(e)
                elif contains_keyword(e.title):
                    entries.append(e)
        return entries[:MAX_ARTICLES]

    # -------------------------------------------------------------------------
    # 기사 본문 크롤링
    # -------------------------------------------------------------------------
    def fetch_article(self, url: str) -> Optional[Dict]:
        try:
            headers = {'User-Agent': USER_AGENT}
            r = requests.get(url, headers=headers, timeout=10)
            r.encoding = 'utf-8'
            soup = BeautifulSoup(r.text, 'html.parser')

            og_img = soup.select_one('meta[property="og:image"]')
            img_url = og_img.get('content', '') if og_img else ''

            content = soup.select_one('article') or soup.select_one('.entry-content')
            if not content:
                return None

            if not img_url:
                first_img = content.select_one('img')
                if first_img:
                    img_url = first_img.get('src', '')

            return {
                'text': content.get_text()[:3000],
                'img_url': img_url,
            }
        except Exception as e:
            logger.error(f"기사 크롤링 실패: {e}")
            return None

    # -------------------------------------------------------------------------
    # Gemini 번역 (운세테크 톤)
    # -------------------------------------------------------------------------
    def translate_article(self, title: str, text: str, source: str) -> Optional[Dict]:

        if source == "coin":
            tone = (
                "이 글은 '운세테크' 블로그용입니다. 가상화폐/BTC 뉴스를 다루되,\n"
                "논리적 분석보다 '지금 사야 할까, 말아야 할까?' 직관적 판단 중심으로 재구성하세요.\n"
                "오행/운기와 연결해서 타이밍 감각을 강조하는 톤으로 작성하세요.\n"
                "예: '목(木)의 기운이 강한 지금, 상승장의 씨앗이 뿌려지고 있습니다.'\n\n"
                "한국화 규칙:\n"
                "- 일본 거래소(bitFlyer, GMOコイン 등) → 업비트, 빗썸으로 대체\n"
                "- 일본 규제/제도 언급 → 한국 가상자산 시장 맥락으로 재구성\n"
                "- 엔화(円) 표기 → 원화(₩) 또는 달러($) 기준으로 환산해서 설명\n"
                "- 일본 고유명사/회사명은 글로벌 맥락으로 치환하거나 생략"
            )
        elif source == "gold":
            tone = (
                "이 글은 '운세테크' 블로그용입니다. 금(골드) 실전 투자 내용을 다루되,\n"
                "'금은 수(水)의 기운을 담은 자산'처럼 오행 관점을 자연스럽게 섞어 쓰세요.\n"
                "'지금 금을 사야 하는 운세적 이유'를 직관적으로 제시하는 톤으로 작성하세요.\n\n"
                "한국화 규칙 (반드시 준수):\n"
                "- 일본 특정 업체명, 기관명은 절대 한국 기관명으로 대체하지 말 것 (허위사실 금지)\n"
                "- 대신 일본 로컬 사례에서 보편적 인사이트를 추출해 한국 독자에게 적용 가능한 교훈으로 재구성\n"
                "  예) '일본 귀금속 업체의 매입 규제 사례' → '금 현물 투자 시 환금성과 유동성을 반드시 확인해야 하는 이유'\n"
                "- 엔화 가격은 달러 기준으로 환산하거나 '국제 금 시세' 개념으로 일반화\n"
                "- 일본 세금/규제 내용 → '금 투자 시 세금과 제도를 미리 파악해야 한다'는 보편적 조언으로 전환\n"
                "- 일본 점포/매장 방문 에피소드 → 금 투자 실전 팁으로 재구성\n"
                "- 원문이 일본 로컬 사례여도 한국 독자가 바로 적용할 수 있는 '금 투자 인사이트'로 재탄생시킬 것"
            )
        else:  # fortune
            tone = (
                "이 글은 '운세테크' 블로그용입니다. 사주/점술 내용을 다루되,\n"
                "재테크/투자 타이밍과 연결하는 관점으로 재구성하세요.\n"
                "예: '이 사주를 가진 사람이 2026년 투자에서 주의해야 할 점'\n\n"
                "한국화 규칙:\n"
                "- 일본 점술 용어는 한국 독자에게 친숙한 표현으로 풀어서 설명\n"
                "- 일본 명절/절기 기준 → 한국 절기(입춘, 경칩 등) 기준으로 대체\n"
                "- 일본 지명/문화 맥락 → 자연스럽게 생략하거나 보편적 표현으로 치환\n"
                "- 점술 내용을 한국 독자의 실생활(주식, 코인, 부동산 타이밍)과 연결"
            )

        prompt = (
            f"{tone}\n\n"
            "아래 규칙을 반드시 지켜서 작성하세요:\n"
            "1. 친근한 존댓말로 작성할 것.\n"
            "2. 글 마지막에 한 줄 결론을 추가할 것 (예: '→ 오늘은 관망, 내일 진입 고려').\n"
            "3. 저자 이름, 저작권 표시(©, (C), ※), 출처 표기 모두 제거.\n"
            "4. img 태그는 절대 포함하지 말 것.\n"
            "5. 상투적인 반복 문구 금지. 기사마다 신선한 표현 사용.\n\n"
            "반드시 아래 형식으로만 답하세요 (다른 설명 없이):\n"
            "[TITLE]한국어 제목 (한 줄, 태그 없이 텍스트만)\n"
            "[CONTENT]<p>본문 HTML 내용</p>\n\n"
            f"원문 제목: {title}\n"
            f"본문: {text}"
        )
        try:
            logger.info(f"Gemini 번역 중: {title[:40]}...")
            response = self.model.generate_content(prompt)
            raw = response.text

            t_match = re.search(r'\[TITLE\]\s*(.*?)\n', raw + '\n', re.IGNORECASE)
            c_match = re.search(r'\[CONTENT\]\s*(.*)', raw, re.DOTALL | re.IGNORECASE)

            t = t_match.group(1).strip() if t_match else title
            c = c_match.group(1).strip() if c_match else raw
            c = re.sub(r'```html|```', '', c).strip()
            c = re.sub(r'<img[^>]*/?>', '', c)
            c = remove_copyright(c)

            return {'title': t, 'content': c}
        except Exception as e:
            logger.error(f"❌ 번역 에러: {e}")
            return None

    # -------------------------------------------------------------------------
    # Supabase 저장
    # -------------------------------------------------------------------------
    def save_to_supabase(self, article_data: Dict) -> bool:
        try:
            res = self.supabase.table(TABLE_NAME) \
                .select('id') \
                .eq('original_url', article_data['link']) \
                .execute()
            if res.data:
                logger.info(f"이미 저장됨 (스킵): {article_data['link']}")
                return False

            self.supabase.table(TABLE_NAME).insert({
                'title':        article_data['title_kr'],
                'content_html': article_data['content_kr'],
                'original_url': article_data['link'],
                'img_url':      article_data['img_url'],
                'status':       'draft',
                'source':       article_data['source'],
                'created_at':   datetime.utcnow().isoformat(),
            }).execute()
            logger.info(f"✅ Supabase 저장: {article_data['title_kr'][:40]}")
            return True
        except Exception as e:
            logger.error(f"❌ Supabase 저장 실패: {e}")
            return False

    # -------------------------------------------------------------------------
    # 메인 실행
    # -------------------------------------------------------------------------
    def run(self):
        logger.info("t-s 크롤러 시작")
        entries = self.collect_entries()

        if not entries:
            logger.info("새로운 기사 없음")
            return

        logger.info(f"수집된 기사: {len(entries)}개")
        saved = 0

        for entry in entries:
            logger.info(f"▶ {entry.title[:50]}")

            data = self.fetch_article(entry.link)
            if not data:
                continue

            source = get_source(entry.link)
            translated = self.translate_article(entry.title, data['text'], source)
            if not translated:
                continue

            article_data = {
                'title_kr':   translated['title'],
                'content_kr': translated['content'],
                'link':       entry.link,
                'img_url':    data['img_url'],
                'source':     source,
            }

            if self.save_to_supabase(article_data):
                self.posted_articles[entry.link] = datetime.now().isoformat()
                with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
                    json.dump(self.posted_articles, f, ensure_ascii=False, indent=2)
                saved += 1

        logger.info(f"완료: {saved}개 저장")


if __name__ == "__main__":
    TSCrawler().run()
