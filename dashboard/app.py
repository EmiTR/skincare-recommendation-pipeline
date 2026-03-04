"""
app.py — Sip and Tint Dashboard
================================
Local Streamlit dashboard that reads recommendations from DynamoDB
and displays top-3 DM dupes for each Flaconi luxury skincare product.

Usage:
    pip install streamlit boto3 pandas
    streamlit run app.py
"""

import streamlit as st
import boto3
import pandas as pd
from decimal import Decimal
from boto3.dynamodb.conditions import Attr

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Sip & Tint — Skincare Dupe Finder",
    page_icon="🧴",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS — soft luxury aesthetic
# ---------------------------------------------------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,300;0,400;1,300&family=DM+Sans:wght@300;400;500&display=swap');

/* Global */
html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}

/* Background */
.stApp {
    background: #faf8f5;
}

/* Hide default streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }

/* Hero title */
.hero-title {
    font-family: 'Cormorant Garamond', serif;
    font-size: 3.2rem;
    font-weight: 300;
    color: #1a1a1a;
    letter-spacing: -0.02em;
    line-height: 1.1;
    margin-bottom: 0.2rem;
}
.hero-subtitle {
    font-family: 'DM Sans', sans-serif;
    font-size: 0.95rem;
    font-weight: 300;
    color: #888;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 2.5rem;
}

/* Stat cards */
.stat-row {
    display: flex;
    gap: 1rem;
    margin-bottom: 2rem;
}
.stat-card {
    background: white;
    border: 1px solid #ede9e3;
    border-radius: 12px;
    padding: 1.2rem 1.6rem;
    flex: 1;
    text-align: center;
}
.stat-number {
    font-family: 'Cormorant Garamond', serif;
    font-size: 2.2rem;
    font-weight: 300;
    color: #1a1a1a;
    line-height: 1;
}
.stat-label {
    font-size: 0.75rem;
    color: #999;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-top: 0.3rem;
}

/* Product card */
.product-header {
    background: white;
    border: 1px solid #ede9e3;
    border-radius: 16px;
    padding: 1.8rem 2rem;
    margin-bottom: 1.5rem;
}
.product-brand {
    font-size: 0.75rem;
    color: #c9a96e;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    font-weight: 500;
    margin-bottom: 0.3rem;
}
.product-name {
    font-family: 'Cormorant Garamond', serif;
    font-size: 1.8rem;
    font-weight: 300;
    color: #1a1a1a;
    margin-bottom: 0.5rem;
}
.product-price {
    font-size: 1.1rem;
    color: #444;
    font-weight: 400;
}

/* Dupe card */
.dupe-card {
    background: white;
    border: 1px solid #ede9e3;
    border-radius: 16px;
    padding: 1.5rem 1.8rem;
    margin-bottom: 1rem;
    position: relative;
    transition: all 0.2s ease;
}
.dupe-card:hover {
    border-color: #c9a96e;
    box-shadow: 0 4px 20px rgba(201, 169, 110, 0.12);
}
.rank-badge {
    position: absolute;
    top: 1.2rem;
    right: 1.5rem;
    width: 2rem;
    height: 2rem;
    background: #f5f0e8;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.75rem;
    color: #c9a96e;
    font-weight: 500;
}
.dupe-brand {
    font-size: 0.7rem;
    color: #aaa;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    margin-bottom: 0.25rem;
}
.dupe-name {
    font-family: 'Cormorant Garamond', serif;
    font-size: 1.3rem;
    font-weight: 400;
    color: #1a1a1a;
    margin-bottom: 0.8rem;
    padding-right: 2.5rem;
}
.metrics-row {
    display: flex;
    gap: 1.5rem;
    align-items: center;
}
.metric {
    display: flex;
    flex-direction: column;
}
.metric-value {
    font-size: 1rem;
    font-weight: 500;
    color: #1a1a1a;
}
.metric-label {
    font-size: 0.7rem;
    color: #bbb;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}
.savings-badge {
    background: #f0faf0;
    color: #2d7a2d;
    border: 1px solid #c3e6c3;
    border-radius: 20px;
    padding: 0.2rem 0.8rem;
    font-size: 0.8rem;
    font-weight: 500;
}
.no-savings-badge {
    background: #fef9f0;
    color: #b8860b;
    border: 1px solid #f0d9a0;
    border-radius: 20px;
    padding: 0.2rem 0.8rem;
    font-size: 0.8rem;
    font-weight: 500;
}

/* Similarity bar */
.sim-bar-bg {
    background: #f0ece6;
    border-radius: 4px;
    height: 4px;
    width: 100%;
    margin-top: 0.8rem;
}
.sim-bar-fill {
    height: 4px;
    border-radius: 4px;
    background: linear-gradient(90deg, #c9a96e, #e8c990);
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: #1a1a1a !important;
}
section[data-testid="stSidebar"] * {
    color: #e8e4dc !important;
}
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stTextInput label {
    color: #888 !important;
    font-size: 0.75rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
}

/* Empty state */
.empty-state {
    text-align: center;
    padding: 4rem 2rem;
    color: #bbb;
}
.empty-state-icon {
    font-size: 3rem;
    margin-bottom: 1rem;
}

/* Link button */
.link-btn {
    display: inline-block;
    margin-top: 0.8rem;
    font-size: 0.78rem;
    color: #c9a96e;
    text-decoration: none;
    border-bottom: 1px solid #c9a96e44;
    padding-bottom: 1px;
    letter-spacing: 0.05em;
}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# DynamoDB connection
# ---------------------------------------------------------------------------
@st.cache_resource
def get_table():
    dynamodb = boto3.resource("dynamodb", region_name="eu-central-1")
    return dynamodb.Table("beauty-boba-dev-recommendations")


@st.cache_data(ttl=300)  # cache for 5 minutes
def load_all_items():
    """Scan entire DynamoDB table and return as list of dicts."""
    table  = get_table()
    items  = []
    kwargs = {}

    # Paginate through all items
    while True:
        response = table.scan(**kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key

    return items


def decimal_to_float(obj):
    """Recursively convert Decimal to float in nested dicts/lists."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    if isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    return obj


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
with st.spinner("Loading recommendations..."):
    try:
        raw_items = load_all_items()
        items     = [decimal_to_float(item) for item in raw_items]
    except Exception as e:
        st.error(f"Could not connect to DynamoDB: {e}")
        st.info("Make sure your AWS credentials are configured: `aws configure`")
        st.stop()

if not items:
    st.error("No recommendations found in DynamoDB. Run the pipeline first.")
    st.stop()

# Build lookup structures
all_brands   = sorted(set(item["flaconi_brand"] for item in items))
brand_to_products = {}
for item in items:
    brand = item["flaconi_brand"]
    if brand not in brand_to_products:
        brand_to_products[brand] = []
    brand_to_products[brand].append(item)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("""
    <div style='padding: 1.5rem 0 2rem 0;'>
        <div style='font-family: Cormorant Garamond, serif; font-size: 1.6rem; 
                    font-weight: 300; color: #e8e4dc; letter-spacing: -0.01em;'>
            Sip & Tint
        </div>
        <div style='font-size: 0.7rem; color: #666; text-transform: uppercase; 
                    letter-spacing: 0.15em; margin-top: 0.2rem;'>
            Dupe Finder
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='font-size:0.7rem; color:#666; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:0.5rem;'>Search</div>", unsafe_allow_html=True)
    search_query = st.text_input(
        "Search",
        placeholder="e.g. La Mer, Moisturizer...",
        label_visibility="collapsed"
    )

    st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)
    st.markdown("<div style='font-size:0.7rem; color:#666; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:0.5rem;'>Browse by Brand</div>", unsafe_allow_html=True)
    selected_brand = st.selectbox(
        "Brand",
        ["All brands"] + all_brands,
        label_visibility="collapsed"
    )

    st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)
    st.markdown("<div style='font-size:0.7rem; color:#666; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:0.5rem;'>Min. Similarity</div>", unsafe_allow_html=True)
    min_similarity = st.slider(
        "Min similarity",
        min_value=0.0, max_value=1.0, value=0.0, step=0.05,
        label_visibility="collapsed"
    )

    st.markdown("<div style='height:2rem'></div>", unsafe_allow_html=True)
    st.markdown(f"<div style='font-size:0.75rem; color:#555;'>{len(items)} products matched</div>", unsafe_allow_html=True)
    if st.button("🔄 Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ---------------------------------------------------------------------------
# Filter items
# ---------------------------------------------------------------------------
filtered = items

if search_query:
    q = search_query.lower()
    filtered = [
        item for item in filtered
        if q in item["flaconi_product_name"].lower()
        or q in item["flaconi_brand"].lower()
    ]

if selected_brand != "All brands":
    filtered = [item for item in filtered if item["flaconi_brand"] == selected_brand]

if min_similarity > 0:
    filtered = [
        item for item in filtered
        if item["top_matches"] and
        float(item["top_matches"][0]["cosine_similarity"]) >= min_similarity
    ]

# Sort by best match similarity descending
filtered.sort(
    key=lambda x: float(x["top_matches"][0]["cosine_similarity"]) if x["top_matches"] else 0,
    reverse=True
)


# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------

# Hero
st.markdown("""
<div class='hero-title'>Find your dupe.</div>
<div class='hero-subtitle'>Luxury skincare · Drugstore alternatives · Ingredient matching</div>
""", unsafe_allow_html=True)

# Stats row
total_products  = len(items)
total_brands    = len(all_brands)
avg_sim         = sum(
    float(item["top_matches"][0]["cosine_similarity"])
    for item in items if item["top_matches"]
) / max(len(items), 1)
avg_savings_list = []
for item in items:
    fp = item.get("flaconi_price_eur")
    if item["top_matches"] and fp:
        dp = item["top_matches"][0].get("dm_price_eur")
        if dp and float(fp) > float(dp):
            avg_savings_list.append(float(fp) - float(dp))
avg_saving = sum(avg_savings_list) / max(len(avg_savings_list), 1)

st.markdown(f"""
<div class='stat-row'>
    <div class='stat-card'>
        <div class='stat-number'>{total_products}</div>
        <div class='stat-label'>Products matched</div>
    </div>
    <div class='stat-card'>
        <div class='stat-number'>{total_brands}</div>
        <div class='stat-label'>Luxury brands</div>
    </div>
    <div class='stat-card'>
        <div class='stat-number'>{avg_sim:.0%}</div>
        <div class='stat-label'>Avg similarity</div>
    </div>
    <div class='stat-card'>
        <div class='stat-number'>€{avg_saving:.0f}</div>
        <div class='stat-label'>Avg savings</div>
    </div>
</div>
""", unsafe_allow_html=True)

# Results
if not filtered:
    st.markdown("""
    <div class='empty-state'>
        <div class='empty-state-icon'>🔍</div>
        <div>No products found. Try a different search or brand filter.</div>
    </div>
    """, unsafe_allow_html=True)
else:
    st.markdown(f"<div style='font-size:0.85rem; color:#aaa; margin-bottom:1.5rem;'>Showing {len(filtered)} product{'s' if len(filtered) != 1 else ''}</div>", unsafe_allow_html=True)

    for item in filtered:
        flaconi_name  = item["flaconi_product_name"]
        flaconi_brand = item["flaconi_brand"]
        flaconi_price = item.get("flaconi_price_eur")
        flaconi_url   = item.get("flaconi_url", "#")
        top_matches   = item.get("top_matches", [])

        price_str = f"€{float(flaconi_price):.2f}" if flaconi_price else "Price unavailable"

        st.markdown(f"""
        <div class='product-header'>
            <div class='product-brand'>{flaconi_brand}</div>
            <div class='product-name'>{flaconi_name}</div>
            <div class='product-price'>{price_str} &nbsp;·&nbsp; <a href='{flaconi_url}' target='_blank' class='link-btn'>View on Flaconi ↗</a></div>
        </div>
        """, unsafe_allow_html=True)

        if not top_matches:
            st.markdown("<div style='color:#bbb; font-size:0.85rem; margin-bottom:1.5rem; padding-left:0.5rem;'>No dupes found for this product.</div>", unsafe_allow_html=True)
            continue

        cols = st.columns(len(top_matches))
        for col, match in zip(cols, top_matches):
            with col:
                dm_name   = match["dm_product_name"]
                dm_brand  = match["dm_brand"]
                dm_price  = match.get("dm_price_eur")
                dm_url    = match.get("dm_url", "#")
                sim       = float(match["cosine_similarity"])
                rank      = int(match["rank"])
                sim_pct   = int(sim * 100)
                sim_width = sim_pct

                # Savings calculation
                savings_html = ""
                if flaconi_price and dm_price:
                    savings = float(flaconi_price) - float(dm_price)
                    if savings > 0:
                        savings_html = f"<span class='savings-badge'>Save €{savings:.2f}</span>"
                    else:
                        savings_html = f"<span class='no-savings-badge'>+€{abs(savings):.2f}</span>"

                dm_price_str = f"€{float(dm_price):.2f}" if dm_price else "—"

                st.markdown(f"""
                <div class='dupe-card'>
                    <div class='rank-badge'>#{rank}</div>
                    <div class='dupe-brand'>{dm_brand}</div>
                    <div class='dupe-name'>{dm_name}</div>
                    <div class='metrics-row'>
                        <div class='metric'>
                            <div class='metric-value'>{dm_price_str}</div>
                            <div class='metric-label'>DM Price</div>
                        </div>
                        <div class='metric'>
                            <div class='metric-value'>{sim_pct}%</div>
                            <div class='metric-label'>Match</div>
                        </div>
                        {savings_html}
                    </div>
                    <div class='sim-bar-bg'>
                        <div class='sim-bar-fill' style='width:{sim_width}%;'></div>
                    </div>
                    <a href='{dm_url}' target='_blank' class='link-btn'>View on DM ↗</a>
                </div>
                """, unsafe_allow_html=True)

        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)
