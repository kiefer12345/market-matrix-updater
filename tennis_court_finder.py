"""
Cheltenham 网球场空位查询
从 ClubSpark (LTA) 的公开 GetVenueSessions 接口抓取各场地未来的空闲时段,
合并相邻空闲时段, 只保留连续 >= MIN_MINUTES 分钟的时段, 输出到 tennis_data.json
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

# ============== 配置区域 ==============
BASE_URL = "https://clubspark.lta.org.uk"
TZ = ZoneInfo("Europe/London")
DAYS_AHEAD = int(os.environ.get("TENNIS_DAYS_AHEAD", "14"))  # Tennis in the Park 可提前14天预订
CHUNK_DAYS = 7  # 每次请求的天数
MIN_MINUTES = 60
OUTPUT_FILE = "tennis_data.json"

# ClubSpark 场馆 (slug 即 clubspark.lta.org.uk/<slug>)
VENUES = [
    {"slug": "CheltenhamTennisInThePark", "name": "Tennis in the Park (Montpellier Gardens / Pittville Park)", "area": "Cheltenham 市中心", "type": "公园"},
    {"slug": "PittvillePark", "name": "Pittville Park", "area": "Pittville", "type": "公园"},
    {"slug": "CharltonKingsTennisPlace", "name": "Charlton Kings Tennis Place (Balcarras)", "area": "Charlton Kings", "type": "俱乐部"},
    {"slug": "NE14Tennis", "name": "NE14 Tennis (Cleeve Sports Centre)", "area": "Bishop's Cleeve", "type": "俱乐部"},
    {"slug": "CheltenhamCivilService", "name": "Cheltenham Tennis Club", "area": "Cheltenham", "type": "俱乐部"},
]

# Session Category: 0 = 可预订空位, 1000 = 已被预订, 其它(教练/俱乐部活动/维护/关闭) 都视为不可用
FREE_CATEGORY = 0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def fetch_sessions(slug, start, end):
    """获取场馆某个日期区间的全部 sessions"""
    url = f"{BASE_URL}/v0/VenueBooking/{slug}/GetVenueSessions"
    params = {
        "resourceID": "",
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "roleId": "",
        "_": str(int(time.time() * 1000)),
    }
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        snippet = " ".join(resp.text[:300].split())
        raise RuntimeError(f"HTTP {resp.status_code}: {snippet}")
    return resp.json()


def merge_ranges(ranges):
    """合并重叠或相接的 (start, end) 区间"""
    merged = []
    for s, e in sorted(ranges):
        if merged and s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    return merged


def subtract_ranges(free, blocked):
    """从空闲区间中扣除被占用区间"""
    result = []
    for fs, fe in free:
        pieces = [[fs, fe]]
        for bs, be in blocked:
            nxt = []
            for ps, pe in pieces:
                if be <= ps or bs >= pe:
                    nxt.append([ps, pe])
                    continue
                if bs > ps:
                    nxt.append([ps, bs])
                if be < pe:
                    nxt.append([be, pe])
            pieces = nxt
        result.extend(pieces)
    return result


def free_blocks(sessions, min_minutes=MIN_MINUTES):
    """由某块场地某天的 sessions 计算连续空闲时段 (分钟, 从午夜算起)"""
    free, blocked, costs = [], [], []
    for s in sessions:
        start, end = s.get("StartTime"), s.get("EndTime")
        if start is None or end is None or end <= start:
            continue
        if s.get("Category") == FREE_CATEGORY:
            free.append((start, end))
            cost = s.get("Cost")
            if isinstance(cost, (int, float)):
                costs.append(cost)
        else:
            blocked.append((start, end))
    blocks = subtract_ranges(merge_ranges(free), merge_ranges(blocked))
    blocks = [(s, e) for s, e in merge_ranges(blocks) if e - s >= min_minutes]
    return blocks, (min(costs) if costs else None)


def parse_venue(data, venue, now):
    """把一个场馆的 API 返回转成空闲时段列表"""
    slots = []
    today = now.date()
    now_min = now.hour * 60 + now.minute
    for res in data.get("Resources") or []:
        court = (res.get("Name") or "Court").strip()
        for day in res.get("Days") or []:
            date_str = (day.get("Date") or "")[:10]
            try:
                date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            if date < today:
                continue
            blocks, cost = free_blocks(day.get("Sessions") or [])
            for start, end in blocks:
                if date == today:
                    if end <= now_min:
                        continue
                    start = max(start, now_min)  # 今天只算剩余时间
                    if end - start < MIN_MINUTES:
                        continue
                slots.append({
                    "venue": venue["slug"],
                    "court": court,
                    "date": date.isoformat(),
                    "start": start,
                    "end": end,
                    "minutes": end - start,
                    "cost": cost,
                })
    return slots


def main():
    now = datetime.now(TZ)
    start_date = now.date()
    end_date = start_date + timedelta(days=DAYS_AHEAD - 1)

    all_slots, venues_out = [], []
    for venue in VENUES:
        info = dict(venue)
        info["bookingUrl"] = f"{BASE_URL}/{venue['slug']}/Booking/BookByDate"
        try:
            courts = set()
            chunk_start = start_date
            while chunk_start <= end_date:
                chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS - 1), end_date)
                data = fetch_sessions(venue["slug"], chunk_start, chunk_end)
                courts.update((r.get("Name") or "").strip() for r in data.get("Resources") or [])
                all_slots.extend(parse_venue(data, venue, now))
                chunk_start = chunk_end + timedelta(days=1)
            info["courts"] = sorted(c for c in courts if c)
            info["ok"] = True
            print(f"✅ {venue['name']}: {len(info['courts'])} 块场地")
        except Exception as e:  # 单个场馆失败不影响其它场馆
            info["ok"] = False
            info["error"] = str(e)[:200]
            print(f"❌ {venue['name']}: {e}")
        venues_out.append(info)

    all_slots.sort(key=lambda s: (s["date"], s["start"], s["venue"], s["court"]))
    out = {
        "updateTime": now.strftime("%Y-%m-%d %H:%M"),
        "timezone": "Europe/London",
        "minMinutes": MIN_MINUTES,
        "daysAhead": DAYS_AHEAD,
        "venues": venues_out,
        "slots": all_slots,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)

    print(f"🎾 共 {len(all_slots)} 个 >= {MIN_MINUTES} 分钟的空闲时段, 已写入 {OUTPUT_FILE}")
    for s in all_slots[:15]:
        print(f"   {s['date']} {s['start'] // 60:02d}:{s['start'] % 60:02d}-"
              f"{s['end'] // 60:02d}:{s['end'] % 60:02d}  {s['venue']} / {s['court']}")

    if not any(v["ok"] for v in venues_out):
        sys.exit("所有场馆都获取失败")


if __name__ == "__main__":
    main()
