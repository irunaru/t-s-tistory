"""
ts_crawler.py
GitHub Actions 전용: coinpost.jp + nakaoka-inc.com + senjutsu.jp RSS 크롤링 → Gemini 번역 → Supabase 저장
대상 사이트: t-s.co.kr (roxic.tistory.com)
콘셉트: 운세테크 — 투자 타이밍 + 코인/금/점술 정보
"""

import os
import json
import feedparser
import logging
import requests
import re
import time
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, Optional
from dotenv import load_dotenv
from supabase import create_client
import google.generativeai as genai

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

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

KEYWORDS = [
    "ビットコイン", "BTC", "仮想通貨", "暗号資産", "ETF", "ステーブルコイン",
    "金", "ゴールド", "資産", "投資", "相場", "価格", "上昇", "下落",
    "四柱推命", "占い", "運勢", "運気", "風水", "五行", "オーラ",
    "紫微斗数", "周易", "奇門遁甲", "宿命", "運命", "タイミング",
]

MAX_ARTICLES = 5
TABLE_NAME = "ts_articles"
HISTORY_FILE = "posted_articles_ts.json"

# -------------------------------------------------------------------------
# 소스 판별
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

    def is_in_supabase(self, url: str) -> bool:
        try:
            res = self.supabase.table(TABLE_NAME) \
                .select('id') \
                .eq('original_url', url) \
                .execute()
            return bool(res.data)
        except Exception:
            return False

    def collect_entries(self):
        feedparser.USER_AGENT = USER_AGENT
        new_entries = []
        fallback_filter = []   # 필터 통과 + history 있음
        fallback_all = []      # 필터 무관 + history 있음 (최후 보충용)
        seen_titles = set()    # 제목 중복 체크용

        for url in FEED_SOURCES:
            feed = feedparser.parse(url)
            logger.info(f"[RSS] {url} → {len(feed.entries)}개")
            for e in feed.entries:
                # 제목 중복 체크
                title_key = e.title.strip()[:50]
                if title_key in seen_titles:
                    logger.info(f"제목 중복 스킵: {title_key}")
                    continue
                seen_titles.add(title_key)

                passes_filter = (
                    "senjutsu.jp" in url or contains_keyword(e.title)
                )
                if e.link not in self.posted_articles:
                    if passes_filter:
                        new_entries.append(e)
                    else:
                        fallback_all.append(e)  # 필터 미통과 새 기사도 최후 보충용으로
                else:
                    if passes_filter:
                        fallback_filter.append(e)
                    else:
                        fallback_all.append(e)

        articles = new_entries[:MAX_ARTICLES]

        # 1차 보충: 필터 통과 과거 기사
        if len(articles) < MAX_ARTICLES:
            needed = MAX_ARTICLES - len(articles)
            logger.info(f"새 기사 {len(articles)}개 → {needed}개 과거 기사(필터 통과)로 보충")
            for e in fallback_filter:
                if needed <= 0:
                    break
                if not self.is_in_supabase(e.link):
                    articles.append(e)
                    needed -= 1

        # 2차 보충: 필터 무관 RSS 전체
        if len(articles) < MAX_ARTICLES:
            needed = MAX_ARTICLES - len(articles)
            logger.info(f"여전히 {needed}개 부족 → RSS 전체로 보충")
            for e in fallback_all:
                if needed <= 0:
                    break
                if not self.is_in_supabase(e.link):
                    articles.append(e)
                    needed -= 1

        logger.info(f"최종 수집: {len(articles)}개")
        return articles

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

            return {'text': content.get_text()[:3000], 'img_url': img_url}
        except Exception as e:
            logger.error(f"기사 크롤링 실패: {e}")
            return None

    def translate_article(self, title: str, text: str, source: str) -> Optional[Dict]:

        if source == "coin":
            tone = (
                "이 글은 '운세테크' 블로그용입니다. 가상화폐/BTC 뉴스를 다루되,\n"
                "논리적 분석보다 '지금 사야 할까, 말아야 할까?' 직관적 판단 중심으로 재구성하세요.\n"
                "한국화 규칙:\n"
                "- 제목과 본문에 일본 기업명/고유명사를 절대 사용하지 말 것\n"
                "- 일본 거래소(bitFlyer, GMOコイン, JPYC 등) → '국내 거래소', '한 스테이블코인 프로젝트' 등으로 익명화\n"
                "- 일본 회사/프로젝트명 → '한 글로벌 기업', '한 투자사' 식으로 일반화\n"
                "- 엔화(円) → 달러($) 기준으로 환산해서 설명"
            )
        elif source == "gold":
            tone = (
                "이 글은 '운세테크' 블로그용입니다. 금(골드) 실전 투자 내용을 다루되,\n"
                "'지금 금을 사야 하는 운세적 이유'를 직관적으로 제시하는 톤으로 작성하세요.\n"
                "한국화 규칙 (반드시 준수):\n"
                "- 제목과 본문에 일본 기업명/고유명사를 절대 사용하지 말 것\n"
                "- 일본 특정 업체명 → '한 귀금속 전문점', '한 금 거래 업체' 식으로 익명화\n"
                "- 일본 로컬 사례에서 보편적 인사이트를 추출해 한국 독자 관점으로 재구성\n"
                "- 엔화 가격은 달러 기준으로 환산하거나 '국제 금 시세' 개념으로 일반화"
            )
        else:  # fortune
            tone = (
                "이 글은 '운세테크' 블로그용입니다. 사주/점술 내용을 다루되,\n"
                "재테크/투자 타이밍과 연결하는 관점으로 재구성하세요.\n"
                "한국화 규칙:\n"
                "- 제목과 본문에 일본 인명/지명/고유명사를 절대 사용하지 말 것\n"
                "- 일본 점술 용어는 한국 독자에게 친숙한 표현으로 풀어서 설명\n"
                "- 점술 내용을 한국 독자의 실생활(주식, 코인, 부동산 타이밍)과 연결"
            )

        prompt = (
            "다음 일본어 기사를 한국어 블로그 포스팅으로 충실하게 번역하세요.\n"
            "이 글은 가상화폐/금 투자 및 운세 블로그의 원고 소재로 사용됩니다.\n\n"
            "【핵심 원칙】\n"
            "원문의 내용, 사실, 수치, 사건을 100% 빠짐없이 전달하는 것이 최우선입니다.\n"
            "오행, 운세 관련 내용은 일절 추가하지 마세요.\n\n"
            f"{tone}\n\n"
            "아래 규칙을 반드시 지켜서 작성하세요:\n"
            "1. 원문의 모든 핵심 정보, 수치, 사건을 충실하게 전달할 것.\n"
            "2. 친근한 존댓말로 자연스러운 한국어로 작성할 것.\n"
            "3. 제목에 원문의 핵심 키워드를 포함할 것.\n"
            "4. 제목에 일본 기업명/고유명사가 절대 들어가지 않도록 할 것.\n"
            "5. 도입부 첫 2문장은 질문형 또는 공감형으로 독자를 잡을 것.\n"
            "6. h2 소제목을 2~3개 포함할 것.\n"
            "7. 글자수 800자 이상으로 작성할 것.\n"
            "8. 글 마지막에 한 줄 결론을 추가할 것 (예: '→ 지금은 관망, 다음 주 진입 고려').\n"
            "9. 저자 이름, 저작권 표시(©, (C), ※), 출처 표기 모두 제거.\n"
            "10. img 태그는 절대 포함하지 말 것.\n\n"
            "반드시 아래 형식으로만 답하세요 (다른 설명 없이):\n"
            "[TITLE]한국어 제목 (한 줄, 태그 없이 텍스트만)\n"
            "[CONTENT]<p>도입부</p><h2>소제목</h2><p>본문 HTML 내용</p>\n\n"
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

            c, t = self.review_article(t, c)
            return {'title': t, 'content': c}
        except Exception as e:
            logger.error(f"❌ 번역 에러: {e}")
            return None

    def review_article(self, title: str, content: str):
        review_prompt = (
            "아래 한국어 블로그 글을 애드센스 수익화 관점에서 검토하고 부족한 부분만 보완하세요.\n"
            "체크 항목:\n"
            "- 제목과 본문에 일본 기업명/고유명사/지명이 있으면 반드시 제거하거나 익명화할 것\n"
            "- 제목에 검색 키워드가 포함되어 있는가\n"
            "- h2 소제목이 2개 이상인가\n"
            "- 글자수가 800자 이상인가\n"
            "- 도입부 첫 문장이 독자를 잡는 질문형/공감형인가\n"
            "- 특정 날짜(X월 X일)가 언급되어 있으면 제거하고 '요즘', '이 시기' 등으로 대체\n"
            "부족한 부분만 보완해서 완성본을 반환하세요. 잘 된 부분은 그대로 두세요.\n\n"
            "반드시 아래 형식으로만 답하세요:\n"
            "[TITLE]제목\n"
            "[CONTENT]본문 HTML\n\n"
            f"[TITLE]{title}\n"
            f"[CONTENT]{content}"
        )
        try:
            logger.info("2차 검수 중... (7초 대기)")
            time.sleep(7)  # 분당 10회 한도 방지
            response = self.model.generate_content(review_prompt)
            raw = response.text

            t_match = re.search(r'\[TITLE\]\s*(.*?)\n', raw + '\n', re.IGNORECASE)
            c_match = re.search(r'\[CONTENT\]\s*(.*)', raw, re.DOTALL | re.IGNORECASE)

            t = t_match.group(1).strip() if t_match else title
            c = c_match.group(1).strip() if c_match else content
            c = re.sub(r'```html|```', '', c).strip()
            c = re.sub(r'<img[^>]*/?>', '', c)
            return c, t
        except Exception as e:
            logger.warning(f"⚠️ 2차 검수 실패 (원본 사용): {e}")
            return content, title

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

    def run(self):
        logger.info("t-s 크롤러 시작")
        entries = self.collect_entries()

        if not entries:
            logger.info("새로운 기사 없음")
            return

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
