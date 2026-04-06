import os
import json
import datetime
from google.cloud import bigquery
import vertexai
from vertexai.generative_models import GenerativeModel

# --- CONFIGURATION ---
PROJECT_ID = "githubleakmonitor-492112"
LOCATION = "us-east4" 
DATASET_ID = "watchtower_db"
TRENDS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.tech_trends"
VIBES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.weekly_vibes"

# Define the Categories and their "Seed" keywords to help with grouping
CATEGORIES = {
    "Frontend & UI": ["react", "vite", "tailwind", "next", "lucide", "recharts", "motion", "vue", "svelte", "shadcn"],
    "Backend & API": ["express", "fastapi", "flask", "uvicorn", "pydantic", "multer", "supabase", "firebase", "auth", "psycopg2"],
    "Data Science & Database": ["pandas", "numpy", "scikit-learn", "torch", "tensorflow", "matplotlib", "seaborn", "postgresql", "redis", "mongodb"],
    "Developer Experience & Testing": ["vitest", "jest", "eslint", "prettier", "typescript", "cypress", "playwright", "dotenv", "tsx", "husky"]
}

def run_vibe_check():
    print("🚀 Starting Daily Categorized Vibe Check...")
    vertexai.init(project=PROJECT_ID, location=LOCATION)
    bq_client = bigquery.Client()

    # 1. FETCH TOP LIBRARIES
    query = f"""
        SELECT library_name, COUNT(*) as count
        FROM `{TRENDS_TABLE}`
        WHERE detected_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        GROUP BY 1 ORDER BY 2 DESC LIMIT 300
    """
    results = bq_client.query(query).result()
    
    noise = {
        'version', 'name', 'scripts', 'type', 'url', 'dependencies', 'devdependencies', 
        'description', 'eslint', 'build', 'main', 'license', 'types', 'typescript', 
        'dev', '@types/node', 'author', 'test', 'repository', 'start', 'lint', 
        'default', 'keywords', 'react-dom', 'private', 'node', 'files','engines', 
        'homepage', 'import', 'exports', 'module', 'require', 'browser', 'directory', 
        'bugs', 'category', 'target', 'language', 'funding', 'access', 'publishconfig', 
        'publishtoclawhub', 'githubusername', 'contributors', 'email', 'openclawversion', 
        '.', './package.json', 'format', 'check', 'typecheck', 'preview', 'pluginapi', 'compat'
    }

    raw_libs = [row.library_name for row in results if row.library_name.lower() not in noise]

    if not raw_libs:
        print("⚠️ No valid libraries found.")
        return

    # 2. ASK GEMINI TO CATEGORIZE AND ANALYZE
    # We send the whole list and the categories, asking for a structured JSON response
    model = GenerativeModel("gemini-2.5-flash-lite")
    
    prompt = f"""
    You are a Technical Industry Analyst. Analyze these GitHub trending libraries: {', '.join(raw_libs[:150])}.
    
    Categorize them into exactly these 4 sectors:
    1. Frontend & UI
    2. Backend & API
    3. Data Science & Database
    4. Developer Experience & Testing
    
    For EACH sector:
    - Identify the top 10 libraries from the provided list.
    - Create a 'Theme' name.
    - Write a 2-sentence 'Vibe' summary.
    
    Return ONLY a JSON list with 4 objects:
    [
      {{"sector": "Frontend & UI", "theme": "...", "summary": "...", "top_libs": ["...", "..."]}},
      ...
    ]
    """

    response = model.generate_content(prompt)
    raw_text = response.text.replace("```json", "").replace("```", "").strip()
    
    try:
        categorized_vibes = json.loads(raw_text)
    except Exception as e:
        print(f"❌ JSON Parsing failed: {e}")
        return

    # 3. PREPARE ROWS FOR BIGQUERY
    today = datetime.date.today().isoformat()
    rows_to_insert = []

    for item in categorized_vibes:
        rows_to_insert.append({
            "vibe_date": today,
            "theme_name": f"[{item['sector']}] {item['theme']}", # Combining sector + theme
            "summary": item['summary'],
            "top_libraries": item['top_libs']
        })

    # 4. INSERT INTO BIGQUERY
    errors = bq_client.insert_rows_json(VIBES_TABLE, rows_to_insert)
    
    if not errors:
        print(f"✅ Successfully saved {len(rows_to_insert)} categorized vibes.")
    else:
        print(f"❌ Errors inserting vibes: {errors}")

if __name__ == "__main__":
    run_vibe_check()