import streamlit as st
import pandas as pd
import numpy as np
from google.cloud import bigquery
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go
from datetime import datetime

# ─────────────────────────────────────────────
# 1. CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="WATCHTOWER | Intelligence Hub",
    layout="wide",
    page_icon="🛡️",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────
# 2. NOISE FILTER  (mirrors vibe_check.py)
# ─────────────────────────────────────────────
NOISE = {
    'version', 'name', 'scripts', 'type', 'url', 'dependencies', 'devdependencies',
    'description', 'eslint', 'build', 'main', 'license', 'types', 'typescript',
    'dev', '@types/node', 'author', 'test', 'repository', 'start', 'lint',
    'default', 'keywords', 'react-dom', 'private', 'node', 'files', 'engines',
    'homepage', 'import', 'exports', 'module', 'require', 'browser', 'directory',
    'bugs', 'category', 'target', 'language', 'funding', 'access', 'publishconfig',
    'publishtoclawhub', 'githubusername', 'contributors', 'email', 'openclawversion',
    '.', './package.json', 'format', 'check', 'typecheck', 'preview', 'pluginapi', 'compat',
}

CATEGORIES = {
    "Frontend & UI":               ["react", "vite", "tailwind", "next", "lucide", "recharts", "motion", "vue", "svelte", "shadcn"],
    "Backend & API":               ["express", "fastapi", "flask", "uvicorn", "pydantic", "multer", "supabase", "firebase", "auth", "psycopg2"],
    "Data Science & Database":     ["pandas", "numpy", "scikit-learn", "torch", "tensorflow", "matplotlib", "seaborn", "postgresql", "redis", "mongodb"],
    "Developer Experience & Testing": ["vitest", "jest", "prettier", "cypress", "playwright", "dotenv", "tsx", "husky", "babel", "webpack"],
}

CAT_COLORS = {
    "Frontend & UI":               "#38bdf8",
    "Backend & API":               "#34d399",
    "Data Science & Database":     "#f472b6",
    "Developer Experience & Testing": "#a78bfa",
}

# ─────────────────────────────────────────────
# 3. CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: #060608 !important;
    color: #e2e8f0;
}
.main .block-container {
    padding: 0rem 1.4rem 0.4rem 1.4rem;
    max-width: 100%;
}
#MainMenu, footer, header { visibility: hidden; }

/* Remove top whitespace Streamlit injects */
.main > div:first-child { padding-top: 0 !important; }
[data-testid="stAppViewBlockContainer"] { padding-top: 0.4rem !important; }

/* Layout wrapper — clean dark shell */
[data-testid="stLayoutWrapper"] {
    border: none !important;
    box-shadow: none !important;
}

.hero-title {
    font-family: 'Space Mono', monospace;
    font-size: 1.6rem;
    font-weight: 700;
    letter-spacing: 0.15em;
    background: linear-gradient(90deg, #38bdf8, #818cf8, #f472b6);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-size: 300% 300%;
    animation: gradientShift 6s ease infinite;
}
@keyframes gradientShift {
    0%   { background-position: 0%   50%; }
    50%  { background-position: 100% 50%; }
    100% { background-position: 0%   50%; }
}

.live-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(239,68,68,0.12);
    border: 1px solid rgba(239,68,68,0.35);
    border-radius: 20px;
    padding: 2px 10px 2px 8px;
    font-size: 0.72rem;
    font-family: 'Space Mono', monospace;
    letter-spacing: 0.08em;
    color: #f87171;
}
.pulse-dot {
    width: 7px; height: 7px;
    border-radius: 50%;
    background: #ef4444;
    animation: pulse 1.4s ease-in-out infinite;
    display: inline-block;
}
@keyframes pulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50%       { opacity: 0.3; transform: scale(0.7); }
}

div[data-testid="stVerticalBlock"] > div:has(div.stMetric) {
    border: 1px solid #1e3a5f;
    padding: 12px 16px;
    border-radius: 10px;
    position: relative;
    overflow: hidden;
}
div[data-testid="stVerticalBlock"] > div:has(div.stMetric)::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, #38bdf8, #818cf8);
    animation: scanline 3s linear infinite;
}
@keyframes scanline {
    0%   { opacity: 0.4; }
    50%  { opacity: 1; }
    100% { opacity: 0.4; }
}

.section-label {
    font-family: 'Space Mono', monospace;
    font-size: 0.7rem;
    letter-spacing: 0.2em;
    color: #64748b;
    text-transform: uppercase;
    margin-bottom: 4px;
    margin-top: 8px;
}

/* Vibe card — full text, no truncation */
.vibe-card {
    background: #0f172a;
    border: 1px solid #1e293b;
    border-radius: 8px;
    padding: 8px 12px;
    margin-bottom: 8px;
}
.vibe-sector {
    font-size: 0.62rem;
    font-family: 'Space Mono', monospace;
    letter-spacing: .1em;
    margin-bottom: 4px;
}
.vibe-summary {
    font-size: 0.73rem;
    color: #94a3b8;
    line-height: 1.55;
    white-space: normal;
    word-break: break-word;
}

/* Category lib table header accent */
.cat-header {
    font-family: 'Space Mono', monospace;
    font-size: 0.68rem;
    letter-spacing: .12em;
    text-transform: uppercase;
    padding: 4px 0 2px 0;
    border-bottom: 1px solid #1e293b;
    margin-bottom: 4px;
}

.js-plotly-plot { margin: 0 !important; }
hr { border-color: #1e293b !important; margin: 6px 0 !important; }
</style>
""", unsafe_allow_html=True)

st_autorefresh(interval=30000, key="global_refresh")

# ─────────────────────────────────────────────
# 4. LAYOUT HELPER
# ─────────────────────────────────────────────
client = bigquery.Client()

BASE = dict(
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    font=dict(color="#94a3b8", family="DM Sans", size=11),
    margin=dict(l=10, r=10, t=30, b=10),
)

def apply_layout(fig, height, **kwargs):
    fig.update_layout(**BASE, height=height, **kwargs)


# ─────────────────────────────────────────────
# 5. QUERIES
# ─────────────────────────────────────────────
@st.cache_data(ttl=30)
def fetch_kpis():
    q = """
        SELECT
            COUNT(DISTINCT commit_sha)   AS total_leaks,
            AVG(entropy_score)           AS avg_risk,
            COUNTIF(entropy_score > 4.5) AS critical_count,
            COUNT(DISTINCT repo_name)    AS affected_repos
        FROM `githubleakmonitor-492112.watchtower_db.detected_leaks`
        WHERE detected_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    """
    return client.query(q).to_dataframe().iloc[0]

@st.cache_data(ttl=30)
def fetch_entropy_volatility():
    q = """
        SELECT TIMESTAMP_TRUNC(detected_at, HOUR) AS hr,
               AVG(entropy_score) AS avg_entropy,
               MAX(entropy_score) AS max_entropy
        FROM `githubleakmonitor-492112.watchtower_db.detected_leaks`
        GROUP BY 1 ORDER BY 1 ASC
    """
    return client.query(q).to_dataframe()

@st.cache_data(ttl=30)
def fetch_entropy_distribution():
    q = """
        SELECT entropy_score
        FROM `githubleakmonitor-492112.watchtower_db.detected_leaks`
        WHERE detected_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    """
    return client.query(q).to_dataframe()

@st.cache_data(ttl=30)
def fetch_top_risky_repos():
    q = """
        SELECT repo_name,
               COUNT(DISTINCT commit_sha) AS leak_count,
               AVG(entropy_score)         AS avg_entropy
        FROM `githubleakmonitor-492112.watchtower_db.detected_leaks`
        WHERE detected_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        GROUP BY 1
        ORDER BY avg_entropy DESC
        LIMIT 10
    """
    df = client.query(q).to_dataframe()
    df['repo_name'] = df['repo_name'].apply(
        lambda x: f"{x.split('/')[0][:4]}**/{x.split('/')[1][:4]}**" if '/' in x else x[:8] + "**"
    )
    df['avg_entropy'] = df['avg_entropy'].round(2)
    return df

@st.cache_data(ttl=30)
def fetch_hourly_heatmap():
    q = """
        SELECT
            EXTRACT(DAYOFWEEK FROM detected_at) AS dow,
            EXTRACT(HOUR     FROM detected_at)  AS hour,
            COUNT(DISTINCT commit_sha)           AS count
        FROM `githubleakmonitor-492112.watchtower_db.detected_leaks`
        GROUP BY 1, 2
    """
    return client.query(q).to_dataframe()

@st.cache_data(ttl=10)
def fetch_live_stream():
    q = """
        SELECT repo_name, commit_sha, entropy_score, detected_at
        FROM (
            SELECT repo_name, commit_sha, entropy_score, detected_at,
                   ROW_NUMBER() OVER (PARTITION BY repo_name ORDER BY detected_at DESC) AS rn
            FROM `githubleakmonitor-492112.watchtower_db.detected_leaks`
        )
        WHERE rn = 1
        ORDER BY detected_at DESC
        LIMIT 50
    """
    df = client.query(q).to_dataframe()
    df['repo_name']     = df['repo_name'].apply(
        lambda x: f"{x.split('/')[0][:3]}**/{x.split('/')[1][:3]}**" if '/' in x else x[:6] + "**"
    )
    df['commit_sha']    = df['commit_sha'].apply(lambda x: x[:8] + "················")
    df['entropy_score'] = df['entropy_score'].round(2)
    df['risk']          = df['entropy_score'].apply(
        lambda x: "🔴 CRITICAL" if x > 4.5 else ("🟡 HIGH" if x > 3.5 else "🟢 LOW")
    )
    df['detected_at']   = df['detected_at'].dt.strftime('%Y-%m-%d %H:%M')
    df = df[['repo_name', 'commit_sha', 'entropy_score', 'risk', 'detected_at']]
    df.columns = ['Repository', 'Commit SHA', 'Entropy', 'Risk', 'Detected At']
    return df

@st.cache_data(ttl=60)
def fetch_tech_trends():
    """All libs, noise-filtered, top 60 by 7-day usage."""
    q = """
        SELECT library_name, COUNT(*) AS usage_count
        FROM `githubleakmonitor-492112.watchtower_db.tech_trends`
        WHERE detected_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 300
    """
    df = client.query(q).to_dataframe()
    df = df[~df['library_name'].str.lower().isin(NOISE)]
    return df.head(60).reset_index(drop=True)

@st.cache_data(ttl=60)
def fetch_category_libs():
    """Top 20 libs per category, noise-filtered, keyword-matched."""
    q = """
        SELECT library_name, COUNT(*) AS usage_count
        FROM `githubleakmonitor-492112.watchtower_db.tech_trends`
        WHERE detected_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 500
    """
    df = client.query(q).to_dataframe()
    df = df[~df['library_name'].str.lower().isin(NOISE)]

    result = {}
    for cat, seeds in CATEGORIES.items():
        mask = df['library_name'].str.lower().apply(
            lambda name: any(kw in name for kw in seeds)
        )
        matched = df[mask].head(20).reset_index(drop=True)
        matched.columns = ['Library', 'Uses (7D)']
        result[cat] = matched
    return result

@st.cache_data(ttl=60)
def fetch_vibe_reports():
    return client.query("""
        SELECT * FROM `githubleakmonitor-492112.watchtower_db.weekly_vibes`
        ORDER BY vibe_date DESC LIMIT 4
    """).to_dataframe()


# ─────────────────────────────────────────────
# 6. RENDER
# ─────────────────────────────────────────────

# ── HERO ─────────────────────────────────────
now = datetime.utcnow().strftime("%H:%M:%S UTC")
st.markdown(
    f'''<div style="display:flex;align-items:center;justify-content:space-between;
                  padding:6px 0 4px 0;border-bottom:1px solid #1e293b;margin-bottom:6px">
        <span class="hero-title">⚡ GITHUB WATCHTOWER  INTELLIGENCE HUB  v1.0</span>
        <span class="live-badge"><span class="pulse-dot"></span>LIVE · {now}</span>
    </div>''',
    unsafe_allow_html=True,
)

# ── KPIs ─────────────────────────────────────
kpi = fetch_kpis()
k1, k2, k3, k4, k5 = st.columns(5)
with k1: st.metric("LEAKS (24H)",     f"{int(kpi['total_leaks']):,}")
with k2: st.metric("AVG ENTROPY",     f"{kpi['avg_risk']:.3f}")
with k3: st.metric("CRITICAL (>4.5)", f"{int(kpi['critical_count']):,}", delta="⚠️ Flagged")
with k4: st.metric("REPOS ANALIZED",  f"{int(kpi['affected_repos']):,}")
with k5: st.metric("STREAM RATE",     "~1.2k/min", delta="Spark")

st.divider()

# ── ROW 1 : Entropy Volatility | Top Risky Repos | Entropy Distribution ──
r1c1, r1c2, r1c3 = st.columns([2.2, 2, 1.8])

with r1c1:
    st.markdown('<p class="section-label">Entropy Volatility (Hourly)</p>', unsafe_allow_html=True)
    df_vol = fetch_entropy_volatility()
    fig_vol = go.Figure()
    fig_vol.add_trace(go.Scatter(
        x=df_vol['hr'], y=df_vol['avg_entropy'],
        name='Avg', fill='tozeroy',
        line=dict(color='#38bdf8', width=1.5),
        fillcolor='rgba(56,189,248,0.07)',
        mode='lines',
    ))
    fig_vol.add_trace(go.Scatter(
        x=df_vol['hr'], y=df_vol['max_entropy'],
        name='Peak',
        line=dict(color='#f472b6', width=1, dash='dot'),
        mode='lines',
    ))
    apply_layout(fig_vol, height=200,
        xaxis=dict(gridcolor='#1e293b', linecolor='#1e293b', showgrid=True),
        yaxis=dict(gridcolor='#1e293b', linecolor='#1e293b', showgrid=True),
        legend=dict(bgcolor='rgba(0,0,0,0)', font=dict(size=10), orientation='h', y=1.05, x=0),
    )
    st.plotly_chart(fig_vol, use_container_width=True)

with r1c2:
    st.markdown('<p class="section-label">Top 10 Riskiest Repos (24H)</p>', unsafe_allow_html=True)
    df_repos = fetch_top_risky_repos()
    fig_repos = go.Figure(go.Bar(
        x=df_repos['avg_entropy'],
        y=df_repos['repo_name'],
        orientation='h',
        marker=dict(
            color=df_repos['avg_entropy'],
            colorscale=[[0, '#1e3a5f'], [0.5, '#818cf8'], [1, '#f472b6']],
            showscale=False,
        ),
        text=df_repos['avg_entropy'].astype(str),
        textposition='outside',
        textfont=dict(size=9, color='#94a3b8'),
    ))
    apply_layout(fig_repos, height=200,
        xaxis=dict(showgrid=False, showticklabels=False),
        yaxis=dict(showgrid=False, tickfont=dict(size=9)),
    )
    st.plotly_chart(fig_repos, use_container_width=True)

with r1c3:
    st.markdown('<p class="section-label">Entropy Distribution</p>', unsafe_allow_html=True)
    df_dist = fetch_entropy_distribution()
    fig_dist = go.Figure(go.Histogram(
        x=df_dist['entropy_score'],
        nbinsx=25,
        marker_color='#818cf8',
        opacity=0.85,
    ))
    fig_dist.add_vline(x=4.5, line_dash='dash', line_color='#ef4444',
                       annotation_text='Critical', annotation_font_size=9,
                       annotation_font_color='#ef4444')
    apply_layout(fig_dist, height=200,
        bargap=0.05,
        xaxis=dict(gridcolor='#1e293b', title=None),
        yaxis=dict(gridcolor='#1e293b', title=None),
    )
    st.plotly_chart(fig_dist, use_container_width=True)

st.divider()

# ── ROW 2 : Heatmap | Radar | Vibe Cards (full text) ──
r2c1, r2c2, r2c3 = st.columns([2.2, 2.2, 1.6])

with r2c1:
    st.markdown('<p class="section-label">Leak Activity Heatmap (DoW × Hour)</p>', unsafe_allow_html=True)
    df_hm = fetch_hourly_heatmap()
    DOW = {1:'Sun', 2:'Mon', 3:'Tue', 4:'Wed', 5:'Thu', 6:'Fri', 7:'Sat'}
    df_hm['day_name'] = df_hm['dow'].map(DOW)
    pivot = df_hm.pivot_table(index='day_name', columns='hour', values='count', fill_value=0)
    pivot = pivot.reindex(['Mon','Tue','Wed','Thu','Fri','Sat','Sun'])
    fig_hm = go.Figure(go.Heatmap(
        z=pivot.values,
        x=[f"{h:02d}h" for h in pivot.columns],
        y=pivot.index.tolist(),
        colorscale=[[0,'#0d1117'],[0.3,'#1e3a5f'],[0.7,'#818cf8'],[1,'#f472b6']],
        showscale=False,
        hoverongaps=False,
    ))
    apply_layout(fig_hm, height=215,
        xaxis=dict(tickfont=dict(size=8), showgrid=False),
        yaxis=dict(tickfont=dict(size=9), showgrid=False),
    )
    st.plotly_chart(fig_hm, use_container_width=True)

with r2c2:
    st.markdown('<p class="section-label">Tech Trend Radar — Top Libraries (7D)</p>', unsafe_allow_html=True)
    df_trends = fetch_tech_trends()
    top_radar = df_trends.head(12)
    fig_radar = go.Figure(go.Scatterpolar(
        r=top_radar['usage_count'],
        theta=top_radar['library_name'],
        fill='toself',
        fillcolor='rgba(129,140,248,0.15)',
        line=dict(color='#818cf8', width=1.5),
        marker=dict(size=4, color='#f472b6'),
    ))
    apply_layout(fig_radar, height=215,
        showlegend=False,
        polar=dict(
            bgcolor='rgba(0,0,0,0)',
            radialaxis=dict(visible=False, gridcolor='#1e293b'),
            angularaxis=dict(tickfont=dict(size=9, color='#94a3b8'), gridcolor='#1e293b'),
        ),
    )
    st.plotly_chart(fig_radar, use_container_width=True)

with r2c3:
    st.markdown('<p class="section-label">Weekly Vibe Summaries</p>', unsafe_allow_html=True)
    vibes = fetch_vibe_reports()
    cards_html = ""
    for _, row in vibes.iterrows():
        sector = (
            row['theme_name'].split(']')[0].replace('[', '').strip()
            if ']' in str(row['theme_name'])
            else str(row['theme_name'])
        )
        color        = CAT_COLORS.get(sector, "#818cf8")
        summary_text = str(row['summary'])
        cards_html += f'''
        <div class="vibe-card">
          <div class="vibe-sector" style="color:{color}">{sector}</div>
          <div class="vibe-summary">{summary_text}</div>
        </div>'''
    st.markdown(
        f'''<div style="height:215px;overflow-y:auto;padding-right:4px">{cards_html}</div>''',
        unsafe_allow_html=True,
    )

st.divider()

# ── ROW 3 : 4 Category Library Tables ────────
st.markdown('<p class="section-label">Top Libraries by Category (7D · Noise-Filtered)</p>', unsafe_allow_html=True)

cat_data = fetch_category_libs()
cat_names = list(CATEGORIES.keys())

cc1, cc2, cc3, cc4 = st.columns(4)
cat_cols = [cc1, cc2, cc3, cc4]

for col, cat in zip(cat_cols, cat_names):
    color = CAT_COLORS[cat]
    df_cat = cat_data.get(cat, pd.DataFrame(columns=['Library', 'Uses (7D)']))
    with col:
        st.markdown(
            f'<div class="cat-header" style="color:{color}">{cat}</div>',
            unsafe_allow_html=True,
        )
        if df_cat.empty:
            st.caption("No data")
        else:
            st.dataframe(
                df_cat,
                use_container_width=True,
                height=220,
                hide_index=True,
                column_config={
                    "Library":    st.column_config.TextColumn(width="medium"),
                    "Uses (7D)":  st.column_config.ProgressColumn(
                        min_value=0,
                        max_value=int(df_cat['Uses (7D)'].max()) if not df_cat.empty else 1,
                        format="%d",
                    ),
                },
            )

st.divider()

# ── ROW 4 : All-libs Table | Live Stream ──
r4c1, r4c2 = st.columns([1.4, 2.6])

with r4c1:
    st.markdown('<p class="section-label">Top 60 Libraries — All Categories (7D)</p>', unsafe_allow_html=True)
    df_lib = fetch_tech_trends().copy()
    df_lib.columns = ['Library', 'Uses (7D)']
    st.dataframe(
        df_lib,
        use_container_width=True,
        height=220,
        hide_index=True,
        column_config={
            "Library":   st.column_config.TextColumn(width="medium"),
            "Uses (7D)": st.column_config.ProgressColumn(
                min_value=0,
                max_value=int(df_lib['Uses (7D)'].max()),
                format="%d",
            ),
        },
    )

with r4c2:
    st.markdown(
        '<p class="section-label">'
        '<span class="live-badge" style="font-size:.62rem">'
        '<span class="pulse-dot"></span>LIVE STREAM — UNIQUE REPOS · LATEST COMMIT ONLY'
        '</span></p>',
        unsafe_allow_html=True,
    )
    live_df = fetch_live_stream()
    st.dataframe(
        live_df,
        use_container_width=True,
        height=220,
        hide_index=True,
        column_config={
            "Entropy": st.column_config.ProgressColumn(
                min_value=0, max_value=8, format="%.2f",
            ),
            "Risk": st.column_config.TextColumn(width="small"),
        },
    )

# ── FOOTER ───────────────────────────────────
st.markdown(
    '<div style="text-align:center;font-size:0.62rem;color:#334155;'
    'font-family:Space Mono,monospace;padding-top:4px">'
    'WATCHTOWER v1.0 · Auto-refresh 30s · Identifiers partially masked · Entropy > 4.5 = Critical'
    '</div>',
    unsafe_allow_html=True,
)
