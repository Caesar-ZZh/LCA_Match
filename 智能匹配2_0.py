import streamlit as st
import pandas as pd
import os
import re
import difflib
import io
import csv

# ================= 网页基础配置 =================
st.set_page_config(page_title="LCA 智能匹配系统 (V39)", page_icon="🌱", layout="wide")

st.title("🌱 LCA 智能匹配系统 (Web版)")
st.markdown("""
### 🚀 使用指南
1. **后台数据**：请确保服务器端已加载所有基础数据库（头表、上游表等）。
2. **上传文件**：请上传需要匹配的 **[模型物料项]** 表格（支持 .xlsx 或 .csv）。
3. **自动处理**：系统将执行 V38 核心算法（冷却水修正、基本流兜底、严格地理过滤）。
4. **结果下载**：匹配完成后，下载标准格式 CSV 文件。
""")

# ================= 0. 后台文件加载器 =================
@st.cache_data
def load_reference_data():
    files_map = {
        "头表": "匹配关系头表.CSV", 
        "上游表": "匹配关系上游背景数据行表.CSV",
        "基本流表": "匹配关系基本流表.CSV",
        "废弃物表": "匹配关系废弃物处置背景数据行表.CSV",
        "副产品表": "匹配关系副产品背景数据行表.CSV",
        "回收利用表": "匹配关系回收利用背景数据行表.CSV"
    }
    
    loaded = {}
    missing = []

    for key, fname in files_map.items():
        if os.path.exists(fname):
            try:
                loaded[key] = pd.read_csv(fname, dtype=str)
            except:
                try:
                    loaded[key] = pd.read_csv(fname, encoding='gbk', dtype=str)
                except:
                    try:
                        # 这里读取后台参考表时也加上 engine='openpyxl' 以防万一
                        loaded[key] = pd.read_excel(fname, dtype=str, engine='openpyxl')
                    except:
                        pass 
        else:
            missing.append(fname)
    
    return loaded, missing

with st.spinner('正在加载后台数据库...'):
    ref_dfs, missing_files = load_reference_data()

st.sidebar.title("📦 数据库状态")
if missing_files:
    st.sidebar.error(f"❌ 缺失文件: {len(missing_files)} 个")
    for f in missing_files:
        st.sidebar.text(f"- {f}")
    st.error("⚠️ 严重错误：后台参考文件缺失，无法运行匹配！请检查文件夹。")
    st.stop()
else:
    st.sidebar.success("✅ 所有参考库加载正常")

# ================= 1. 核心算法 (V38逻辑) =================

def process_matching(df_model, ref_dfs):
    df_header = ref_dfs['头表']
    bg_dfs = {
        'Upstream': ref_dfs['上游表'], 'Waste': ref_dfs['废弃物表'],
        'Byprod': ref_dfs['副产品表'], 'Recycle': ref_dfs['回收利用表'], 'Elementary': ref_dfs['基本流表']
    }

    progress_bar = st.progress(0, text="正在索引背景数据...")
    
    h_name_col = next((c for c in df_header.columns if '名称' in c and '中文' in c), '物料项名称（中文）')
    h_id_col = next((c for c in df_header.columns if '匹配关系ID' in c), '匹配关系ID')
    df_header['clean'] = df_header[h_name_col].astype(str).str.strip()
    header_map = df_header.set_index('clean')[h_id_col].astype(str).str.strip().to_dict()
    
    bg_id_map = {}
    bg_name_list = {'Upstream': [], 'Waste': [], 'Byprod': [], 'Recycle': [], 'Elementary': []}
    
    total_cats = len(bg_dfs)
    curr = 0
    for cat, df in bg_dfs.items():
        lid_col = next((c for c in df.columns if '匹配关系ID' in c), None)
        if cat == 'Elementary':
            name_col = next((c for c in df.columns if '基本流名称' in c and '中文' in c), '基本流名称（中文）')
            unit_col = next((c for c in df.columns if '单位' in c), '单位（英文）')
            loc_col = next((c for c in df.columns if '分类' in c), '基本流分类') 
            fact_col, ref_col = None, None
        else:
            name_col = next((c for c in df.columns if '名称' in c and '中文' in c), '名称')
            unit_col = next((c for c in df.columns if '单位' in c), '单位')
            loc_col = next((c for c in df.columns if '地理位置' in c), '地理位置')
            fact_col = next((c for c in df.columns if '碳足迹' in c), '碳足迹')
            ref_col = next((c for c in df.columns if '参考产品' in c), None)
        
        id_col = 'ID'
        for _, row in df.iterrows():
            item = {
                'ID': str(row.get(id_col, '')).strip(),
                '碳足迹': str(row.get(fact_col, '')) if fact_col else "N/A",
                '单位': str(row.get(unit_col, '')).strip(),
                '地理位置': str(row.get(loc_col, '')).strip(),
                '背景名称': str(row.get(name_col, '')).strip(),
                '参考产品': str(row.get(ref_col, '')).strip() if ref_col else "",
                '来源': cat
            }
            if lid_col:
                lid = str(row[lid_col]).strip()
                if lid not in bg_id_map: bg_id_map[lid] = []
                bg_id_map[lid].append(item)
            bg_name_list[cat].append(item)
        curr += 1
        progress_bar.progress(int(curr/total_cats * 20), text="正在索引背景数据...")

    STRICT_LOCATIONS = {
        '中国', 'cn', 'china', '全球', 'glo', 'global',
        'row', 'rest of world', '世界其他地区', '未指定', 'unspecified'
    }
    SYNONYMS_MAP = {
        '河水': ['地表水', 'surface water', 'water, river', '河', 'river'],
        '湖水': ['地表水', 'surface water', 'water, lake', '湖', 'lake'],
        '雨水': ['地表水', 'surface water', 'water, rain', '雨'],
        '冷却水': ['自来水', 'tap water'], '循环水': ['自来水', 'tap water']
    }
    SPECIAL_RULES = {'一般工业固废': '43274789141377048'}

    def clean_name_str(s): return re.sub(r'\(.*?\)|（.*?）', '', s).strip()
    def string_similarity(s1, s2): return difflib.SequenceMatcher(None, s1.lower(), s2.lower()).ratio()
    def check_unit(m_unit, bg_unit): return "一致" if m_unit == bg_unit else "不一致"

    def get_score(item, m_name, m_cat):
        loc = item['地理位置']
        bg_name = item['背景名称'].lower()
        ref_prod = item['参考产品'].lower()
        source = item['来源']
        m_name_clean = clean_name_str(m_name).lower()
        
        if '冷却水' in m_name or '循环水' in m_name:
            if ('自来水' in bg_name or 'tap water' in bg_name):
                return 999 if ('市场' in bg_name or 'market' in bg_name) else 500
        
        score = 10 
        if source == 'Elementary':
            score = 50
            if '未指定' in loc or 'unspecified' in loc.lower(): score += 30
            if '水' in m_name:
                if '未指定的天然来源' in bg_name or 'unspecified natural origin' in bg_name: score += 20
                if '地表' in loc or 'surface' in loc.lower(): score += 15
                if '河' in m_name and 'river' in bg_name: score += 40 
                if '湖' in m_name and 'lake' in bg_name: score += 40 
            if m_cat == '大气排放' and '空气' in loc: score += 10
            elif m_cat == '水体排放' and '水' in loc: score += 10
            sim = string_similarity(m_name, item['背景名称'])
            score += sim * 5
            return score
        
        if 'hiq' in bg_name and loc=='中国': score = 100
        elif loc=='中国': score = 90
        elif '世界其他地区' in loc or 'RoW' in loc: score = 80
        elif '全球' in loc: score = 70
        
        if len(ref_prod) > 1 and (m_name_clean in ref_prod or ref_prod in m_name_clean): score += 20
        if any(k in bg_name for k in ['未指定','unspecified','不指定','平均','通用','混合']): score += 25
        if any(k in bg_name for k in ['生产','production','制造']): score += 10
        if m_cat in ['废弃物', '副产品']:
            whitelist = ['处理','处置','焚烧','填埋','回收','再利用','treatment','disposal']
            if '生产' in bg_name and not any(w in bg_name for w in whitelist): score -= 40
        
        sim = string_similarity(m_name_clean, clean_name_str(item['背景名称']).lower())
        score += sim * 10 
        return score

    result_data = []
    total_rows = len(df_model)
    progress_bar.progress(30, text="AI 正在匹配中...")
    
    for idx, row in df_model.iterrows():
        if idx % 5 == 0:
            prog = 30 + int((idx / total_rows) * 60)
            progress_bar.progress(min(prog, 99), text=f"正在匹配: {row.get('物料项名称（中文）', '')}")

        m_name = str(row.get('物料项名称（中文）', '')).strip()
        m_cat = str(row.get('物料项类别', '')).strip()
        m_type = str(row.get('物料项类型', ''))
        m_attr = str(row.get('物料项属性', ''))
        
        candidates = []
        if m_name in SPECIAL_RULES:
            cands = bg_id_map.get(SPECIAL_RULES[m_name])
            if cands: candidates.extend(cands)
        
        lid = header_map.get(m_name)
        if lid and lid in bg_id_map:
            cands = bg_id_map[lid]
            if m_cat in ['自然资源输入', '大气排放', '水体排放']:
                candidates.extend([c for c in cands if c['来源'] == 'Elementary'])
            else:
                candidates.extend([c for c in cands if c['来源'] != 'Elementary'])
        
        search_terms = [m_name, clean_name_str(m_name)]
        if m_name in SYNONYMS_MAP: search_terms.extend(SYNONYMS_MAP[m_name])
        
        target_cats = []
        if m_cat in ['原辅料', '能源及能源介质']: target_cats = ['Upstream']
        elif m_cat == '废弃物': target_cats = ['Waste']
        elif m_cat == '副产品': target_cats = ['Byprod']
        elif m_cat == '回收利用': target_cats = ['Recycle']
        elif m_cat in ['自然资源输入', '大气排放', '水体排放']: target_cats = ['Elementary']
        
        is_natural = any(x in m_name for x in ['水', '河', '湖', '雨', '井', '气', '土', '资源'])
        if is_natural or not candidates:
            if 'Elementary' not in target_cats: target