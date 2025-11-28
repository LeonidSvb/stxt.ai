#!/usr/bin/env python3
"""
Instagram Lead Enrichment - Main Application
Modular lead enrichment system via Streamlit UI
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import os
from datetime import datetime
from dotenv import load_dotenv
import asyncio
from modules.enrichment_workflow import EnrichmentWorkflow

load_dotenv()

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

st.set_page_config(
    page_title="Instagram Enrichment",
    page_icon="📸",
    layout="wide"
)

PRESETS = {
    "Safe": {
        "name": "🛡️ Safe (1 req/sec)",
        "description": "Safe mode - matches RapidAPI rate limit",
        "batch_size": 1,
        "delay": 1.0,
        "queries_per_lead": 2
    },
    "Balanced": {
        "name": "⚖️ Balanced (0.8 req/sec)",
        "description": "Balanced - reliable with margin",
        "batch_size": 5,
        "delay": 1.2,
        "queries_per_lead": 1
    }
}

if 'enriched_df' not in st.session_state:
    st.session_state.enriched_df = None
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'stats' not in st.session_state:
    st.session_state.stats = None

st.title("📸 Instagram Lead Enrichment")
st.markdown("Modular lead enrichment system via RapidAPI Google Search")

tab1, tab2, tab3 = st.tabs(["⚙️ Settings", "📊 Results", "📖 Documentation"])

with tab1:
    col_left, col_right = st.columns([1, 2])

    with col_left:
        st.subheader("⚙️ Configuration")

        st.markdown("### 🎛️ Presets")
        st.info("⚠️ RapidAPI limit: **1 request/second**")

        preset_choice = st.radio(
            "Select mode",
            options=list(PRESETS.keys()),
            format_func=lambda x: PRESETS[x]["name"],
            index=0
        )

        preset = PRESETS[preset_choice]
        st.info(preset["description"])

        with st.expander("🔧 Advanced Settings"):
            st.markdown("**Performance**")

            batch_size = st.number_input(
                "Batch Size",
                min_value=1,
                max_value=100,
                value=preset["batch_size"],
                help="Number of leads per batch (recommended 1-10 for rate limit 1 req/sec)"
            )

            delay = st.slider(
                "Delay between requests (sec)",
                min_value=0.5,
                max_value=5.0,
                value=preset["delay"],
                step=0.1,
                help="Minimum 1.0 sec for RapidAPI (1 req/sec)"
            )

            if delay < 1.0:
                st.warning("⚠️ Delay < 1 sec may cause rate limit!")

            st.markdown("**Query Templates**")

            use_custom_query = st.checkbox(
                "Use custom query",
                value=False,
                help="Disable for automatic strategy (email -> name)"
            )

            if use_custom_query:
                st.markdown("**Available variables:**")
                st.code("{name} - lead name\n{email} - lead email")

                query_template = st.text_area(
                    "Query template",
                    value='"{email}" instagram',
                    height=100,
                    help="Example: \"{email}\" instagram\nExample: \"{name}\" instagram influencer"
                )

                st.markdown("**Examples:**")
                st.code('"{email}" instagram')
                st.code('"{name}" instagram')
                st.code('"{name}" "{email}" instagram')
            else:
                query_template = None
                st.info("Strategy: email first, then name (fallback)")

        st.markdown("### 🔑 API")
        rapidapi_key = st.text_input(
            "RapidAPI Key",
            value=os.getenv("RAPIDAPI_KEY", ""),
            type="password"
        )

        st.markdown("### 📊 Processing Limits")
        process_all = st.checkbox("Process all rows", value=False)
        if not process_all:
            max_rows = st.number_input(
                "Number of rows",
                min_value=1,
                max_value=1000,
                value=10
            )
        else:
            max_rows = None

    with col_right:
        st.subheader("📁 Upload and Run")

        # Show existing results for resume
        UPLOADS_DIR = Path(__file__).parent / "uploads"
        UPLOADS_DIR.mkdir(exist_ok=True)

        results_files = sorted(RESULTS_DIR.glob("enriched_*.csv"), key=os.path.getmtime, reverse=True)

        if results_files:
            st.markdown("### 🔄 Resume from Previous Run")

            with st.expander(f"📋 {len(results_files)} previous session(s) available", expanded=False):
                for result_file in results_files[:5]:  # Show last 5
                    try:
                        df_check = pd.read_csv(result_file, encoding='utf-8-sig')
                        total = len(df_check)
                        found = len(df_check[df_check.get('Status') == 'Found']) if 'Status' in df_check.columns else 0
                        not_found = len(df_check[df_check.get('Status') == 'Not Found']) if 'Status' in df_check.columns else 0
                        pending = total - found - not_found

                        col_info, col_btn = st.columns([3, 1])
                        with col_info:
                            st.text(f"📄 {result_file.name}")
                            st.text(f"   ✅ {found} found | ❌ {not_found} not found | ⏳ {pending} pending")
                        with col_btn:
                            if st.button(f"📂 Load", key=f"load_{result_file.name}"):
                                st.session_state.resume_file = result_file
                                st.rerun()
                    except Exception as e:
                        st.text(f"⚠️ {result_file.name} (error reading)")

        st.markdown("### 📤 Upload New or Continue")

        # Check if resuming
        resume_mode = False
        if 'resume_file' in st.session_state and st.session_state.resume_file:
            resume_mode = True
            st.info(f"🔄 Resuming from: {st.session_state.resume_file.name}")
            df = pd.read_csv(st.session_state.resume_file, encoding='utf-8-sig')
            uploaded_file = None
        else:
            uploaded_file = st.file_uploader(
                "CSV with leads",
                type=['csv'],
                help="Upload CSV with columns: Person - Name, Person - Email - Work"
            )

        if uploaded_file or resume_mode:
            if not resume_mode:
                df = pd.read_csv(uploaded_file, encoding='utf-8-sig')

            # Show statistics
            total = len(df)
            found = len(df[df.get('Instagram URL', '') != '']) if 'Instagram URL' in df.columns else 0
            pending = total - found

            st.success(f"✅ Loaded {total} rows ({found} already found, {pending} pending)")

            if resume_mode:
                if st.button("🔄 Clear and Upload New"):
                    del st.session_state.resume_file
                    st.rerun()

            with st.expander("👀 Preview (first 10 rows)"):
                st.dataframe(df.head(10))

            col1, col2 = st.columns(2)
            with col1:
                name_col = st.selectbox(
                    "Column: Name",
                    df.columns,
                    index=0 if 'Person - Name' not in df.columns else list(df.columns).index('Person - Name')
                )
            with col2:
                email_col = st.selectbox(
                    "Column: Email",
                    df.columns,
                    index=2 if len(df.columns) > 2 else 0
                )

            st.markdown("### 📊 Estimates")
            estimated_leads = len(df) if process_all else min(max_rows or 10, len(df))
            estimated_requests = estimated_leads * (2 if not use_custom_query else 1)
            estimated_time_sec = estimated_requests * delay
            estimated_time_min = estimated_time_sec / 60

            col1, col2, col3 = st.columns(3)
            col1.metric("Leads", estimated_leads)
            col2.metric("API requests", f"~{estimated_requests}")
            col3.metric("Time (min)", f"~{estimated_time_min:.1f}")

            st.markdown("### 🚀 Launch")

            if st.button("🚀 START ENRICHMENT", type="primary", use_container_width=True, disabled=st.session_state.processing):
                if not rapidapi_key:
                    st.error("❌ Please provide RapidAPI key!")
                else:
                    st.session_state.processing = True

                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    results_placeholder = st.empty()

                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

                    # If resuming, use existing file; otherwise create new
                    if resume_mode:
                        temp_input = str(st.session_state.resume_file)
                        output_file = st.session_state.resume_file
                        status_text.info(f"🔄 Resuming from existing file...")
                    else:
                        # Save uploaded file to uploads/
                        if uploaded_file:
                            upload_filename = uploaded_file.name.replace('.csv', f'_{timestamp}.csv')
                            upload_path = UPLOADS_DIR / upload_filename
                            df.to_csv(upload_path, index=False, encoding='utf-8-sig')

                        temp_input = f"temp_input_{timestamp}.csv"
                        output_file = RESULTS_DIR / f"enriched_{timestamp}.csv"

                        df_renamed = df.rename(columns={
                            name_col: 'Person - Name',
                            email_col: 'Person - Email - Work'
                        })
                        df_renamed.to_csv(temp_input, index=False, encoding='utf-8-sig')

                    try:
                        status_text.info("⚙️ Initializing workflow...")

                        workflow = EnrichmentWorkflow(
                            api_key=rapidapi_key,
                            batch_size=batch_size,
                            delay=delay
                        )

                        def update_progress(processed, total):
                            progress_bar.progress(processed / total)
                            status_text.info(f"Processed: {processed}/{total}")

                        status_text.info("🚀 Starting enrichment...")

                        stats = asyncio.run(
                            workflow.enrich_csv(
                                input_csv=temp_input,
                                output_csv=str(output_file),
                                max_leads=max_rows,
                                query_template=query_template if use_custom_query else None,
                                progress_callback=update_progress
                            )
                        )

                        # Only remove temp file if not resuming
                        if not resume_mode and os.path.exists(temp_input):
                            os.remove(temp_input)

                        # Clear resume state after successful processing
                        if resume_mode and 'resume_file' in st.session_state:
                            del st.session_state.resume_file

                        progress_bar.progress(1.0)
                        status_text.success("✅ Enrichment completed!")

                        st.session_state.enriched_df = pd.read_csv(output_file, encoding='utf-8-sig')
                        st.session_state.stats = stats

                        col1, col2, col3 = st.columns(3)
                        col1.metric("Processed", stats['processed'])
                        col2.metric("Found", stats['found'])
                        col3.metric("Success Rate", f"{stats['found']/stats['processed']*100:.1f}%")

                        st.dataframe(st.session_state.enriched_df.head(20))

                        csv_data = st.session_state.enriched_df.to_csv(index=False, encoding='utf-8-sig')
                        st.download_button(
                            "⬇️ Download results",
                            data=csv_data,
                            file_name=f"enriched_{timestamp}.csv",
                            mime='text/csv'
                        )

                    except Exception as e:
                        status_text.error(f"❌ Error: {e}")
                        import traceback
                        st.error(traceback.format_exc())

                    finally:
                        st.session_state.processing = False

with tab2:
    st.subheader("📊 Results History")

    results_files = sorted(RESULTS_DIR.glob("*.csv"), key=os.path.getmtime, reverse=True)

    if results_files:
        selected_file = st.selectbox(
            "Select file",
            options=results_files,
            format_func=lambda x: f"{x.name} ({datetime.fromtimestamp(x.stat().st_mtime).strftime('%Y-%m-%d %H:%M')})"
        )

        if selected_file:
            df_history = pd.read_csv(selected_file, encoding='utf-8-sig')

            total = len(df_history)
            found = len(df_history[df_history.get('Status') == 'Found']) if 'Status' in df_history.columns else 0

            col1, col2, col3 = st.columns(3)
            col1.metric("Total", total)
            col2.metric("Found", found)
            col3.metric("Success Rate", f"{found/total*100:.1f}%" if total > 0 else "0%")

            st.dataframe(df_history, use_container_width=True)

            csv_buffer = df_history.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                "⬇️ Download",
                data=csv_buffer,
                file_name=selected_file.name,
                mime='text/csv'
            )
    else:
        st.info("📭 No saved results")

with tab3:
    st.subheader("📖 Documentation")

    st.markdown("""
    ## 🎯 Quick Start

    1. **Upload CSV** with leads (columns: Person - Name, Person - Email - Work)
    2. **Select preset** (Safe for beginners)
    3. **Configure parameters** (optional)
    4. **Start enrichment**

    ## 📊 Presets

    ### 🛡️ Safe (Recommended)
    - **Delay:** 1.0 sec (strictly matches RapidAPI limit)
    - **Batch:** 1 lead
    - **Queries:** 2 per lead (email + name fallback)
    - **Speed:** ~30 leads/hour
    - **Risk:** Minimal

    ### ⚖️ Balanced
    - **Delay:** 1.2 sec (with margin)
    - **Batch:** 5 leads
    - **Queries:** 1 per lead
    - **Speed:** ~50 leads/hour
    - **Risk:** Low

    ## 🔧 Settings

    ### Batch Size
    Number of leads processed in parallel in async mode.
    - **1-5:** Optimal for rate limit 1 req/sec
    - **Higher:** Increased blocking risk

    ### Delay
    Delay between API requests.
    - **Minimum 1.0 sec** for RapidAPI (1 request/second)
    - **Recommended 1.1-1.2 sec** for safety

    ### Custom Query Template
    Allows full control over search query.

    **Variables:**
    - `{name}` - lead name
    - `{email}` - lead email

    **Examples:**
    ```
    "{email}" instagram
    "{name}" instagram influencer
    "{name}" "{email}" instagram site:instagram.com
    ```

    **When to use:**
    - For specific niches (e.g., "fitness instagram")
    - For precise targeting
    - For experimenting with different strategies

    ## ⚡ RapidAPI Limits

    **Your plan:**
    - 500,000 requests/month
    - **1 request/second**
    - Bandwidth: 10 GB/month

    **Recommendations:**
    - Use delay ≥ 1.0 sec
    - Start with small tests (10-50 leads)
    - Monitor rate limit in logs

    ## 🐛 Troubleshooting

    **Rate Limit Error:**
    - Increase delay to 1.5-2.0 sec
    - Reduce batch size to 1
    - Wait 1-2 minutes between runs

    **Low Success Rate:**
    - Use custom query template
    - Try strategy with 2 queries (email + name)
    - Check data quality in CSV

    **Timeout Errors:**
    - Check internet connection
    - Verify RapidAPI key validity
    """)

st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Status")
if st.session_state.processing:
    st.sidebar.success("🟢 Processing...")
else:
    st.sidebar.info("⚪ Ready to run")

if st.session_state.stats:
    st.sidebar.markdown("### 📈 Last Run")
    st.sidebar.metric("Found", st.session_state.stats['found'])
    st.sidebar.metric("API requests", st.session_state.stats['api_requests'])
